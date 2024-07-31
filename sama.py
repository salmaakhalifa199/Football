from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import from_json
from pyspark.sql.types import*
import pymysql
from pyspark.sql.functions import col

#conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database)
#cursor = conn.cursor()

def insert_into_phpmyadmin(row):
    # Define the connection details for your PHPMyAdmin database
    host = "localhost"
    port = 3306
    database = "big_data"
    username = "root"
    password = ""
    
    conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database)
    cursor = conn.cursor()

    # Extract the required columns from the row
   

    # Prepare the SQL query to insert data into the table
    #sql_query =f"INSERT INTO `p1`(`Team`, `Tournament`, `Goals`, `Shots`, `Yellow_Cards`, `Red_Cards`, `Possession`, `Pass`, `AerialsWon`, `Rating`) VALUES ('{row.Team}','{row.Goals}','{row.Shots}','{row.yellow_cards}','{row.red_cards}','{row.Possession}','{row.Pass}','{row.AerialsWon}','{row.Rating}')"
    sql_query=f"INSERT INTO `football`(`Tournament`,`Possession`) VALUES ('{row.Tournament}','{row.Avggoal}')"
    # Execute the SQL query
    print('submit into data base') 
    cursor.execute(sql_query)

    # Commit the changes
    conn.commit()
    conn.close()

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# Define the schema for your DataFrame
schema = StructType().add("Team", StringType()).add("Tournament", StringType()).add("Goals", IntegerType()).add("shots", FloatType()).add("yellow_cards",IntegerType()).add("red_cards",IntegerType()).add("Possession",FloatType()).add("Pass",FloatType()).add("AerialsWon",FloatType()).add("Rating",FloatType())



# Read data from Kafka topic as a DataFrame
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \

# Select specific columns from "data"
#df = df.select("data.*")
df = df.select("data.Tournament","data.Goals","data.Rating","data.Team")
grouped_df = df.groupBy('Tournament').agg(avg('Goals').alias('Avggoal'))
ordered_df = grouped_df.orderBy(col('AvgGoal').desc())

#filtered_df = df.filter(col("Pass%") > 85)

#.agg(sum('Goals').alies('sumgoal'))
#possison_df =df.groupBy('Team').filter(col('Pass') > 85).select('Team')
#grouped_df = df.groupBy('Team').agg({'Pass'})
#red_card_df = df.groupBy('Team').agg(sum('Red_Cards').alias('TotalRedCards')).orderBy(desc('TotalRedCards')).limit(5)
# Filter teams with average pass percentage above 85%
#high_pass_teams = grouped_df.filter(col('avg(Pass)') > 85).select('Team')
#sorted_grouped_df = grouped_df.orderBy(desc('Avggoal'))
#filter_=df.filter('Avggoal>52').select((['Tournament','Goals']))
# Convert the value column to string and display the result
query =ordered_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .foreach(insert_into_phpmyadmin) \ 
    .start()

# Wait for the query to finish
query.awaitTermination()
# query2=filtered_df.writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .foreach(insert_into_phpmyadmin) \
#     .start()
#    .foreach(insert_into_phpmyadmin) \
