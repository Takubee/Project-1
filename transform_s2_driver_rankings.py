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
    col("response.behind").alias("behind"),
    col("response.driver.abbr").alias("driver_abbr"),
    col("response.driver.id").alias("driver_id"),
    col("response.driver.image").alias("driver_image"),
    col("response.driver.name").alias("driver_name"),
    col("response.driver.number").alias("driver_number"),
    col("response.points").alias("points"),
    col("response.position").alias("position"),
    col("response.season").alias("season"),
    col("response.team.id").alias("team_id"),
    col("response.team.logo").alias("team_logo"),
    col("response.team.name").alias("team_name"),
    col("response.wins").alias("wins")
)

# View Dataframe

getColumns.show()

# Path to save the flattened data in S3
s3_output_path = "s3a://s3-bucket-location"

# Write the flattened DataFrame to S3
getColumns.write.format("parquet").mode("overwrite").save(s3_output_path)

print("complete")
