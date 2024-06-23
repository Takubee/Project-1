from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, posexplode


# Initialize Spark session builder
spark_builder = SparkSession.builder.appName("MySparkSession")

# Create the Spark session
spark = spark_builder.getOrCreate()

# Path to your JSON file in S3
s3_input_path = "s3a://file-pathn"

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
s3_output_path = "s3a://s3-bucket-location/"

# Write the flattened DataFrame to S3
getColumns.write.format("parquet").mode("overwrite").save(s3_output_path)

print("complete")


