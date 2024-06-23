from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, posexplode

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

getColumns = exploded_df.withColumn("response1", explode("response.response")) \
                    .withColumn("abbr", col("response1.abbr")) \
                    .withColumn("birthdate", col("response1.birthdate")) \
                    .withColumn("birthplace", col("response1.birthplace")) \
                    .withColumn("country_code", col("response1.country.code")) \
                    .withColumn("country_name", col("response1.country.name")) \
                    .withColumn("grands_prix_entered", col("response1.grands_prix_entered")) \
                    .withColumn("highest_grid_position", col("response1.highest_grid_position")) \
                    .withColumn("highest_race_finish_number", col("response1.highest_race_finish.number")) \
                    .withColumn("highest_race_finish_position", col("response1.highest_race_finish.position")) \
                    .withColumn("id", col("response1.id")) \
                    .withColumn("image", col("response1.image")) \
                    .withColumn("name", col("response1.name")) \
                    .withColumn("nationality", col("response1.nationality")) \
                    .withColumn("number", col("response1.number")) \
                    .withColumn("podiums", col("response1.podiums")) \
                    .withColumn("world_championships", col("response1.world_championships")) \
                    .drop("response1","response", "response", "errors", "get", "parameters", "results")

getColumns.show()


# Path to save the flattened data in S3
s3_output_path = "s3a://s3-bucket-location"

# Write the flattened DataFrame to S3
getColumns.write.format("parquet").mode("overwrite").save(s3_output_path)

print("complete")


