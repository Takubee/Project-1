from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

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

# Select the nested fields explicitly
getColumns = exploded_df.select(
    col("response.circuit.id").alias("circuit_id"),
    col("response.circuit.image").alias("circuit_image"),
    col("response.circuit.name").alias("circuit_name"),
    col("response.competition.id").alias("competition_id"),
    col("response.competition.location.city").alias("city"),
    col("response.competition.location.country").alias("country"),
    col("response.competition.name").alias("competition_name"),
    col("response.date").alias("date"),
    col("response.distance").alias("distance"),
    col("response.fastest_lap.driver.id").alias("fastest_lap_driver_id"),
    col("response.fastest_lap.time").alias("fastest_lap_time"),
    col("response.id").alias("id"),
    col("response.laps.current").alias("current_lap"),
    col("response.laps.total").alias("total_laps"),
    col("response.season").alias("season"),
    col("response.status").alias("status"),
    col("response.timezone").alias("timezone"),
    col("response.type").alias("type"),
    col("response.weather").alias("weather")

)

# View Dataframe

getColumns.show()

# Path to save the flattened data in S3
s3_output_path = "s3a://s3-bucket-location/"

# Write the flattened DataFrame to S3
getColumns.write.format("parquet").mode("overwrite").save(s3_output_path)

print("complete")


