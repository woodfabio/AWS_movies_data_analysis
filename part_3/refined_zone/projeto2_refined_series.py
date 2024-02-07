import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col

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
# read data from S3 and create DynamicFrames

dynamic_frame = glueContext.create_dynamic_frame.from_options(
    "s3",
    {
        "paths": [
            source_file
        ]
    },
    "parquet"
    )

# --------------------------------------------------------------------------
# specify useless columns to remove
columns_to_remove = ["title", "artist_gender", "character", "artist_name", "birth_year", "death_year"]
dynamic_frame = DropFields.apply(frame = dynamic_frame, paths = columns_to_remove)

# --------------------------------------------------------------------------
# turn DynamicFrame into spark DataFrame
spark_df = dynamic_frame.toDF()

# --------------------------------------------------------------------------
# cast the "runtime" column from string to integer
spark_df = spark_df.withColumn("runtime", col("runtime").cast("int"))

# --------------------------------------------------------------------------
# repartition the DataFrame to a single partition
spark_df = spark_df.repartition(1)

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
sink.setCatalogInfo(catalogDatabase="dl_refined_zone", catalogTableName="unified_series_table")
sink.setFormat("parquet", useGlueParquetWriter=True)
sink.writeFrame(new_dynamic_frame)

job.commit()