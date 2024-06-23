from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

# Initialize Spark session builder
spark_builder = SparkSession.builder.appName("MySparkSession")

# Create the Spark session
spark = spark_builder.getOrCreate()

# Path to your JSON file in S3
s3_input_path = "s3a://your-s3-bucket-location/your-file-name.json"

# Load JSON data from S3
df = spark.read.format("json")\
            .option("multiLine", True)\
            .option("header", True)\
            .option("inferschema", True)\
            .load(s3_input_path)

df.printSchema()

print("done")
# Explode the 'response' array
exploded_df = df.withColumn("response", explode("response"))

# Select the nested fields explicitly
flattened_df = exploded_df.select(
    col("response.capacity").alias("capacity"),
    col("response.competition.id").alias("competition_id"),
    col("response.competition.location.city").alias("competition_location_city"),
    col("response.competition.location.country").alias("competition_location_country"),
    col("response.competition.name").alias("competition_name"),
    col("response.first_grand_prix").alias("first_grand_prix"),
    col("response.id").alias("id"),
    col("response.image").alias("image"),
    col("response.lap_record.driver").alias("lap_record_driver"),
    col("response.lap_record.time").alias("lap_record_time"),
    col("response.lap_record.year").alias("lap_record_year"),
    col("response.laps").alias("laps"),
    col("response.length").alias("length"),
    col("response.name").alias("name")
)

flattened_df.show(truncate=False)


# Path to save the flattened data in S3
s3_output_path = "s3a://your-s3-bucket-location/"

# Write the flattened DataFrame to S3
flattened_df.write.format("parquet").mode("overwrite").save(s3_output_path)

print("complete")
