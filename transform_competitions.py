from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

# Initialize Spark session builder
spark_builder = SparkSession.builder.appName("MySparkSession")

# Create the Spark session
spark = spark_builder.getOrCreate()

# Path to your JSON file in S3
s3_input_path = "s3a://file-path"

# Load JSON data from S3
df = spark.read.format("json").option("multiLine", True).load(s3_input_path)

df.printSchema()

exploded_df = df.withColumn("response", explode("response"))

exploded_df.printSchema()

# Select the nested fields explicitly
getColumns = exploded_df.select(
    col("response.id").alias("id"),
    col("response.location.city").alias("city"),
    col("response.location.country").alias("country"),
    col("response.name").alias("name")
)

# View Dataframe

getColumns.show()

# Path to save the flattened data in S3
s3_output_path = "s3a://s3-bucket-location"

# Write the flattened DataFrame to S3
getColumns.write.format("parquet").mode("overwrite").save(s3_output_path)

print("complete")
