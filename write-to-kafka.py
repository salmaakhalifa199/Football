from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# Define the schema for your DataFrame
schema = StructType().add("Team", StringType()).add("Tournament", StringType()).add("Goals", IntegerType()).add("shots", FloatType()).add("yellow_cards",IntegerType()).add("red_cards",IntegerType()).add("Possession",FloatType()).add("Pass",FloatType()).add("AerialsWon",FloatType()).add("Rating",FloatType())


# Read data from a directory as a streaming DataFrame
streaming_df = spark.readStream \
    .format("csv") \
    .schema(schema) \
    .option("path", "C:/football_project/project14/data") \
    .option("header", "true") \
    .load() 
        

# Select specific columns from "data"
#df = streaming_df.select("name", "age")

#df = streaming_df.select(col("name").alias("key"), to_json(col("age")).alias("value"))
df = streaming_df.select(to_json(struct("*")).alias("value"))

# Convert the value column to string and display the result
query = df.selectExpr("CAST(value AS STRING)") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "test") \
    .option("checkpointLocation", "null") \
    .start()

# Wait for the query to finish
query.awaitTermination()
