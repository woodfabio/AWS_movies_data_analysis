import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import explode, col
from pyspark.sql import functions as F
from pyspark.sql.window import Window

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH_LOCAL', 'S3_INPUT_PATH_API_1', 'S3_INPUT_PATH_API_2', 'S3_INPUT_PATH_API_3', 'S3_TARGET_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --------------------------------------------------------------------------
# define paths
source_file_local = args['S3_INPUT_PATH_LOCAL']
source_file_api_1 = args['S3_INPUT_PATH_API_1']
source_file_api_2 = args['S3_INPUT_PATH_API_2']
source_file_api_3 = args['S3_INPUT_PATH_API_3']
target_path = args['S3_TARGET_PATH']

# --------------------------------------------------------------------------
# read data from S3 and create DynamicFrames

# local
dynamic_frame_local = glueContext.create_dynamic_frame.from_options(
    "s3",
    {
        "paths": [
            source_file_local
        ]
    },
    "parquet"
    )

# API 1
dynamic_frame_api_1 = glueContext.create_dynamic_frame.from_options(
    "s3",
    {
        "paths": [
            source_file_api_1
        ]
    },
    "parquet"
    )

# API 2
dynamic_frame_api_2 = glueContext.create_dynamic_frame.from_options(
    "s3",
    {
        "paths": [
            source_file_api_2
        ]
    },
    "parquet"
    )

# API 3
dynamic_frame_api_3 = glueContext.create_dynamic_frame.from_options(
    "s3",
    {
        "paths": [
            source_file_api_3
        ]
    },
    "parquet"
    )
    
# --------------------------------------------------------------------------
# specify useless columns to remove

# local
local_columns_to_remove = ["title", "artist_gender", "character", "artist_name", "birth_year", "death_year"]
dynamic_frame_local = DropFields.apply(frame = dynamic_frame_local, paths = local_columns_to_remove)

# APIs
api_columns_to_remove = ["homepage", "overview", "poster_path", "tagline", "title", "video", "genre_id", "company_id", "production_country_iso_3166_1", "spoken_language_iso_639_1", "spoken_language_name"]
dynamic_frame_api_1 = DropFields.apply(frame = dynamic_frame_api_1, paths = api_columns_to_remove)
dynamic_frame_api_2 = DropFields.apply(frame = dynamic_frame_api_2, paths = api_columns_to_remove)
dynamic_frame_api_3 = DropFields.apply(frame = dynamic_frame_api_3, paths = api_columns_to_remove)

# --------------------------------------------------------------------------
# change API DynamicFrames column names for uniformization
columns_to_rename  = {"genre_name": "genre"}
                
for old_name, new_name in columns_to_rename.items():
    dynamic_frame_api_1 = dynamic_frame_api_1.rename_field(old_name, new_name)

for old_name, new_name in columns_to_rename.items():
    dynamic_frame_api_2 = dynamic_frame_api_2.rename_field(old_name, new_name)

for old_name, new_name in columns_to_rename.items():
    dynamic_frame_api_3 = dynamic_frame_api_3.rename_field(old_name, new_name)

# --------------------------------------------------------------------------
# turn DynamicFrames into spark DataFrames
spark_df_local = dynamic_frame_local.toDF()
spark_df_api_1 = dynamic_frame_api_1.toDF()
spark_df_api_2 = dynamic_frame_api_2.toDF()
spark_df_api_3 = dynamic_frame_api_3.toDF()

# --------------------------------------------------------------------------
# repartition the DataFrames to a single partition
spark_df_local = spark_df_local.repartition(1)
spark_df_api_1 = spark_df_api_1.repartition(1)
spark_df_api_2 = spark_df_api_2.repartition(1)
spark_df_api_3 = spark_df_api_3.repartition(1)

# --------------------------------------------------------------------------
# Run Spark SQL queries in API tables to group by "imdb_id" and collect_list "company_name"

spark_df_api_1.createOrReplaceTempView("spark_df_api_1")
collected_cn_api_1 = spark.sql("""
    SELECT
        imdb_id,
        COLLECT_LIST(DISTINCT company_name) AS companies_names
    FROM
        spark_df_api_1
    GROUP BY
        imdb_id
""")
collected_cn_api_1 = collected_cn_api_1.dropDuplicates(collected_cn_api_1.columns)
collected_cn_api_1.createOrReplaceTempView("collected_cn_api_1")

spark_df_api_2.createOrReplaceTempView("spark_df_api_2")
collected_cn_api_2 = spark.sql("""
    SELECT
        imdb_id,
        COLLECT_LIST(DISTINCT company_name) AS companies_names
    FROM
        spark_df_api_2
    GROUP BY
        imdb_id
""")
collected_cn_api_2 = collected_cn_api_2.dropDuplicates(collected_cn_api_2.columns)
collected_cn_api_2.createOrReplaceTempView("collected_cn_api_2")

spark_df_api_3.createOrReplaceTempView("spark_df_api_3")
collected_cn_api_3 = spark.sql("""
    SELECT
        imdb_id,
        COLLECT_LIST(DISTINCT company_name) AS companies_names
    FROM
        spark_df_api_3
    GROUP BY
        imdb_id
""")
collected_cn_api_3 = collected_cn_api_3.dropDuplicates(collected_cn_api_3.columns)
collected_cn_api_3.createOrReplaceTempView("collected_cn_api_3")

# --------------------------------------------------------------------------
#Unify API data

unified_api = spark.sql("""
    SELECT * FROM collected_cn_api_1
    UNION ALL
    SELECT * FROM collected_cn_api_2
    UNION ALL
    SELECT * FROM collected_cn_api_3;
""")
unified_api.createOrReplaceTempView("unified_api")

# --------------------------------------------------------------------------
# Run SQL INSERT INTO statement to add companies_names to "spark_df_local" as "final_table"
spark_df_local.createOrReplaceTempView("spark_df_local")
joined_table_cn_1 = spark.sql("""
    SELECT
        spark_df_local.*,
        unified_api.companies_names
    FROM
        spark_df_local
    LEFT JOIN
        unified_api
    ON
        spark_df_local.imdb_id = unified_api.imdb_id
""")
joined_table_cn_1 = joined_table_cn_1.dropDuplicates(joined_table_cn_1.columns)
joined_table_cn_1.createOrReplaceTempView("joined_table_cn_1")

# --------------------------------------------------------------------------
# un-nest nested columns and split them if they have more than one value

spark.catalog.dropTempView("collected_cn_api_1")
spark.catalog.dropTempView("collected_cn_api_2")
spark.catalog.dropTempView("collected_cn_api_3")
spark.catalog.dropTempView("unified_api")

# explode the arrays in "companies_names" into separate rows
joined_table_cn_1 = joined_table_cn_1.withColumn("company_name", explode("companies_names"))
joined_table_cn_1 = joined_table_cn_1.drop("companies_names")
joined_table_cn_1 = joined_table_cn_1.dropDuplicates(joined_table_cn_1.columns)
joined_table_cn_1.createOrReplaceTempView("joined_table_cn_1")

print("Schema of joined_table_cn_1:")
joined_table_cn_1.printSchema()

# UNTIL HERE THE SCRIPT WORKS, NO EXTRA ROWS ====================================================================================

# --------------------------------------------------------------------------
# Run SQL INSERT INTO statement to add company_origin_country to "joined_table_cn_1" as "joined_table_cc_3"

joined_table_cc_1 = spark.sql("""
    SELECT DISTINCT
        joined_table_cn_1.*,
        spark_df_api_1.company_origin_country
    FROM
        joined_table_cn_1
    LEFT JOIN
        spark_df_api_1
    ON
        joined_table_cn_1.company_name = spark_df_api_1.company_name
""")
joined_table_cc_1 = joined_table_cc_1.dropDuplicates(joined_table_cc_1.columns)
joined_table_cc_1.createOrReplaceTempView("joined_table_cc_1")

joined_table_cc_2 = spark.sql("""
    SELECT DISTINCT
        joined_table_cc_1.*,
        spark_df_api_2.company_origin_country
    FROM
        joined_table_cc_1
    LEFT JOIN
        spark_df_api_2
    ON
        joined_table_cc_1.company_name = spark_df_api_2.company_name
""")
joined_table_cc_2 = joined_table_cc_2.dropDuplicates(joined_table_cc_2.columns)
joined_table_cc_2.createOrReplaceTempView("joined_table_cc_2")

joined_table_cc_3 = spark.sql("""
    SELECT DISTINCT
        joined_table_cc_2.*,
        spark_df_api_3.company_origin_country
    FROM
        joined_table_cc_2
    LEFT JOIN
        spark_df_api_3
    ON
        joined_table_cc_2.company_name = spark_df_api_3.company_name
""")
joined_table_cc_3 = joined_table_cc_3.dropDuplicates(joined_table_cc_3.columns)
joined_table_cc_3.createOrReplaceTempView("joined_table_cc_3")

print("Schema of joined_table_cc_3:")
joined_table_cc_3.printSchema()

# --------------------------------------------------------------------------
# Run Spark SQL queries in API tables to group by "imdb_id" and collect_list "production_countries_names"

collected_pc_api_1 = spark.sql("""
    SELECT
        imdb_id,
        COLLECT_LIST(DISTINCT production_country_name) AS production_countries_names
    FROM
        spark_df_api_1
    GROUP BY
        imdb_id
""")
collected_pc_api_1 = collected_pc_api_1.dropDuplicates(collected_pc_api_1.columns)
collected_pc_api_1.createOrReplaceTempView("collected_pc_api_1")

collected_pc_api_2 = spark.sql("""
    SELECT
        imdb_id,
        COLLECT_LIST(DISTINCT production_country_name) AS production_countries_names
    FROM
        spark_df_api_2
    GROUP BY
        imdb_id
""")
collected_pc_api_2 = collected_pc_api_2.dropDuplicates(collected_pc_api_2.columns)
collected_pc_api_2.createOrReplaceTempView("collected_pc_api_2")

collected_pc_api_3 = spark.sql("""
    SELECT
        imdb_id,
        COLLECT_LIST(DISTINCT production_country_name) AS production_countries_names
    FROM
        spark_df_api_3
    GROUP BY
        imdb_id
""")
collected_pc_api_3 = collected_pc_api_3.dropDuplicates(collected_pc_api_3.columns)
collected_pc_api_3.createOrReplaceTempView("collected_pc_api_3")

# --------------------------------------------------------------------------
#Unify API data

unified_api_2 = spark.sql("""
    SELECT * FROM collected_pc_api_1
    UNION ALL
    SELECT * FROM collected_pc_api_2
    UNION ALL
    SELECT * FROM collected_pc_api_3;
""")
unified_api_2 = unified_api_2.dropDuplicates(unified_api_2.columns)
unified_api_2.createOrReplaceTempView("unified_api_2")

# --------------------------------------------------------------------------
# Run SQL INSERT INTO statement to add production_countries_names to "joined_table_cc_3" as "joined_table_pc_1"

joined_table_pc_1 = spark.sql("""
    SELECT DISTINCT
        joined_table_cc_3.*,
        unified_api_2.production_countries_names
    FROM
        joined_table_cc_3
    LEFT JOIN
        unified_api_2
    ON
        joined_table_cc_3.imdb_id = unified_api_2.imdb_id
""")
joined_table_pc_1 = joined_table_pc_1.dropDuplicates(joined_table_pc_1.columns)
joined_table_pc_1.createOrReplaceTempView("joined_table_pc_1")

# --------------------------------------------------------------------------
# un-nest nested columns and split them if they have more than one value

spark.catalog.dropTempView("collected_pc_api_1")
spark.catalog.dropTempView("collected_pc_api_2")
spark.catalog.dropTempView("collected_pc_api_3")
spark.catalog.dropTempView("unified_api_2")

# explode the arrays in "production_countries_names" into separate rows
joined_table_pc_1 = joined_table_pc_1.withColumn("production_country_name", explode("production_countries_names"))
joined_table_pc_1 = joined_table_pc_1.drop("production_countries_names")
joined_table_pc_1 = joined_table_pc_1.dropDuplicates(joined_table_pc_1.columns)
joined_table_pc_1.createOrReplaceTempView("joined_table_pc_1")

print("Schema of joined_table_pc_1:")
joined_table_pc_1.printSchema()

# --------------------------------------------------------------------------
# cast the "runtime" column from string to integer
joined_table_pc_1 = joined_table_pc_1.withColumn("runtime", col("runtime").cast("int"))

# --------------------------------------------------------------------------
# repartition the DataFrames to a single partition
joined_table_pc_1 = joined_table_pc_1.repartition(1)

# --------------------------------------------------------------------------
# turn DataFrames into glue DynamicFrames
new_dynamic_frame_local = DynamicFrame.fromDF(spark_df_local, glueContext, "new_dynamic_frame_local")
new_dynamic_frame_api_1 = DynamicFrame.fromDF(spark_df_api_1, glueContext, "new_dynamic_frame_api_1")
new_dynamic_frame_api_2 = DynamicFrame.fromDF(spark_df_api_2, glueContext, "new_dynamic_frame_api_2")
new_dynamic_frame_api_3 = DynamicFrame.fromDF(spark_df_api_3, glueContext, "new_dynamic_frame_api_3")

unified_movies_table = DynamicFrame.fromDF(joined_table_pc_1, glueContext, "unified_movies_table")

# --------------------------------------------------------------------------
# Save table as Parquet in S3
#glueContext.write_dynamic_frame.from_options(
#    frame = new_test,
#    connection_type = "s3",
#    connection_options = {"path": target_path},
#    format = "parquet"
#)

# save table in S3 and register in Glue Catalog
sink = glueContext.getSink(
    connection_type = "s3", 
    path = target_path,
    enableUpdateCatalog = True, 
    updateBehavior = "UPDATE_IN_DATABASE",
    partitionKeys=[]
    )
sink.setCatalogInfo(catalogDatabase="dl_refined_zone", catalogTableName="unified_movies_table")
sink.setFormat("parquet", useGlueParquetWriter=True)
sink.writeFrame(unified_movies_table)

job.commit()