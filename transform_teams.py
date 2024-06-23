import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, posexplode

# Initialize a session using Boto3
session = boto3.Session()
credentials = session.get_credentials()
current_credentials = credentials.get_frozen_credentials()

# Initialize Spark session builder
spark_builder = SparkSession.builder.appName("MySparkSession")

# Add S3 configuration for Spark session
spark_builder.config("spark.hadoop.fs.s3a.access.key", current_credentials.access_key)
spark_builder.config("spark.hadoop.fs.s3a.secret.key", current_credentials.secret_key)
spark_builder.config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")

# Conditionally set the session token if it is available
if current_credentials.token:
    spark_builder.config("spark.hadoop.fs.s3a.session.token", current_credentials.token)

# Create the Spark session
spark = spark_builder.getOrCreate()

# Path to your JSON file in S3
s3_input_path = "s3a://formula-1-raw/static/teams.json"

# Load JSON data from S3
df = spark.read.format("json").option("multiLine", True).load(s3_input_path)

df.printSchema()

exploded_df = df.withColumn("response", explode("response"))

exploded_df.printSchema()

# Explicitly select columns from exploded nested json
getColumns = exploded_df.select(
    col("response.base").alias("base"),
    col("response.chassis").alias("chassis"),
    col("response.director").alias("director"),
    col("response.engine").alias("engine"),
    col("response.fastest_laps").alias("fastest_laps"),
    col("response.first_team_entry").alias("first_team_entry"),
    col("response.highest_race_finish.number").alias("highest_race_finish_number"),
    col("response.highest_race_finish.position").alias("highest_race_finish_position"),
    col("response.id").alias("id"),
    col("response.logo").alias("logo"),
    col("response.name").alias("name"),
    col("response.pole_positions").alias("pole_positions"),
    col("response.president").alias("president"),
    col("response.technical_manager").alias("technical_manager"),
    col("response.tyres").alias("tyres"),
    col("response.world_championships").alias("world_championships")
)

# View Dataframe
getColumns.show()

# Path to save the flattened data in S3
s3_output_path = "s3a://formula-1-cleaned/static-cleaned/teams-cleaned/"

# Write the flattened DataFrame to S3
getColumns.write.format("parquet").mode("overwrite").save(s3_output_path)

print("complete")


