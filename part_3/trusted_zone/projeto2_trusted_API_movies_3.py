import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import explode, col, when

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
#dynamic_frame = DynamicFrame.fromDF(spark_df, glueContext, "dynamic_frame")

dynamic_frame = glueContext.create_dynamic_frame.from_options(
    "s3", 
    connection_options={
        "paths": [source_file],
        "recurse": True
    }, 
    format="json",
    format_options={"jsonPath": "$[*]"}
)

# --------------------------------------------------------------------------
# specify useless columns to remove
columns_to_remove = ["backdrop_path", "belongs_to_collection", "id"]
dynamic_frame = DropFields.apply(frame = dynamic_frame, paths = columns_to_remove)

# --------------------------------------------------------------------------
# turn DynamicFrame into spark DataFrame
spark_df = dynamic_frame.toDF()

# --------------------------------------------------------------------------
# un-nest nested columns and split them if they have more than one value

# "genres" column
spark_df = spark_df.select("*", explode("genres").alias("genre"))
spark_df = spark_df.withColumn("genre_id", col("genre.id"))
spark_df = spark_df.withColumn("genre_name", col("genre.name"))
spark_df = spark_df.drop("genres", "genre")

# "production_companies" column
spark_df = spark_df.select("*", explode("production_companies").alias("production_company"))
spark_df = spark_df.withColumn("company_id", col("production_company.id"))
spark_df = spark_df.withColumn("company_logo_path", col("production_company.logo_path"))
spark_df = spark_df.withColumn("company_name", col("production_company.name"))
spark_df = spark_df.withColumn("company_origin_country", col("production_company.origin_country"))
spark_df = spark_df.drop("production_companies", "production_company", "company_logo_path")

# "production_countries" column
spark_df = spark_df.select("*", explode("production_countries").alias("production_country"))
spark_df = spark_df.withColumn("production_country_iso_3166_1", col("production_country.iso_3166_1"))
spark_df = spark_df.withColumn("production_country_name", col("production_country.name"))
# =====
# uniformize column "production_country_name"
spark_df = spark_df.withColumn("production_country_name", when(col("production_country_name") == "United States of America", "US").otherwise(col("production_country_name")))
# =====
spark_df = spark_df.drop("production_countries", "production_country")

# "spoken_languages" column
spark_df = spark_df.select("*", explode("spoken_languages").alias("spoken_language"))
spark_df = spark_df.withColumn("spoken_language_english_name", col("spoken_language.english_name"))
spark_df = spark_df.withColumn("spoken_language_iso_639_1", col("spoken_language.iso_639_1"))
spark_df = spark_df.withColumn("spoken_language_name", col("spoken_language.name"))
spark_df = spark_df.drop("spoken_languages", "spoken_language")

print("="*50)
spark_df.printSchema()
print("="*50)

# --------------------------------------------------------------------------
# turn DataFrame into glue DynamicFrame
new_dynamic_frame = DynamicFrame.fromDF(spark_df, glueContext, "new_dynamic_frame")

# --------------------------------------------------------------------------
# save table in S3 and register in Glue Catalog
sink = glueContext.getSink(
    connection_type = "s3", 
    path = target_path,
    enableUpdateCatalog = True, 
    updateBehavior = "UPDATE_IN_DATABASE",
    partitionKeys=[]
    )
sink.setCatalogInfo(catalogDatabase="dl_trusted_zone", catalogTableName="trusted_api_movies_3")
sink.setFormat("parquet", useGlueParquetWriter=True)
sink.writeFrame(new_dynamic_frame)

job.commit()