from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, from_csv
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Define the schema of your CSV data
schema = StructType([
  StructField("aircraft", StringType()),
  StructField("delay", DoubleType())
])

# Create a SparkSession object
spark = SparkSession\
    .builder\
    .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0")\
    .appName("KafkaStreamExample").getOrCreate()

# Read the CSV data from Kafka and parse it into a DataFrame
kafkaStream = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "aircraft") \
  .load()

df = kafkaStream \
  .selectExpr("CAST(value AS STRING)") \
  .select(from_csv("value", schema).alias("data")) \
  .select("data.*")

# Group the DataFrame by aircraft and sum up the delay values
result = df \
  .groupBy("aircraft") \
  .agg(sum("delay").alias("total_delay"))

# Start the streaming query and print the results to the console
query = result \
  .writeStream \
  .outputMode("complete") \
  .format("console") \
  .start()
query.awaitTermination()
