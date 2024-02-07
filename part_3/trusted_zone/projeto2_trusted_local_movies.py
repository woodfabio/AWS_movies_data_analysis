import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import explode, col, split

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_TARGET_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --------------------------------------------------------------------------
# define paths
source_file = args['S3_INPUT_PATH']
target_path = args['S3_TARGET_PATH']

# --------------------------------------------------------------------------
# read data from S3 and create DynamicFrame
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    "s3",
    {
        "paths": [
            source_file
        ]
    },
    "csv",
    {"withHeader": True, "separator":"|"},
    )
    
# --------------------------------------------------------------------------
# specify useless columns to remove
columns_to_remove = ["titulosMaisConhecidos"]
dynamic_frame = DropFields.apply(frame = dynamic_frame, paths = columns_to_remove)

# --------------------------------------------------------------------------
# Change column names to english and snake case for uniformization with other tables in the layer
columns_to_rename  = {"id": "imdb_id",
                        "tituloPincipal": "title", 
                        "tituloOriginal": "original_title", 
                        "anoLancamento": "release_year",
                        "tempoMinutos": "runtime",
                        "genero": "genre",
                        "notaMedia": "vote_average",
                        "numeroVotos": "vote_count",
                        "generoArtista": "artist_gender",
                        "personagem": "character",
                        "nomeArtista": "artist_name",
                        "anoNascimento": "birth_year",
                        "anoFalecimento": "death_year",
                        "profissao": "profession"
                    }
                
for old_name, new_name in columns_to_rename.items():
    dynamic_frame = dynamic_frame.rename_field(old_name, new_name)
    
print("="*50)
dynamic_frame.printSchema()
print("="*50)

# --------------------------------------------------------------------------
# turn DynamicFrame into spark DataFrame
spark_df = dynamic_frame.toDF()
spark_df.printSchema()

# --------------------------------------------------------------------------
# filter movies with Eric Roberts
eric_roberts_movies = spark_df.filter(spark_df['artist_name'] == 'Eric Roberts')
eric_roberts_movies.show()

# --------------------------------------------------------------------------
# un-nest nested columns and split them if they have more than one value

# split the "genre" column and explode the resulting array into separate rows
eric_roberts_movies = eric_roberts_movies.withColumn("genre", explode(split("genre", ",")))

# split the "profession" column and explode the resulting array into separate rows
eric_roberts_movies = eric_roberts_movies.withColumn("profession", explode(split("profession", ",")))

print("="*50)
print("="*50)
eric_roberts_movies.show()
print("="*50)
print("="*50)
                
# --------------------------------------------------------------------------
# repartition the DataFrame to a single partition
eric_roberts_movies = eric_roberts_movies.repartition(1)

# --------------------------------------------------------------------------
# turn DataFrame into glue DynamicFrame
new_dynamic_frame = DynamicFrame.fromDF(eric_roberts_movies, glueContext, "new_dynamic_frame")

# --------------------------------------------------------------------------
# save table in S3 and register in Glue Catalog
sink = glueContext.getSink(
    connection_type = "s3", 
    path = target_path,
    enableUpdateCatalog = True, 
    updateBehavior = "UPDATE_IN_DATABASE",
    partitionKeys=[]
    )
sink.setCatalogInfo(catalogDatabase="dl_trusted_zone", catalogTableName="trusted_local_movies")
sink.setFormat("parquet", useGlueParquetWriter=True)
sink.writeFrame(new_dynamic_frame)

job.commit()