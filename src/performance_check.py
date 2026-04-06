import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Spark-plugin
spark = SparkSession.builder \
    .appName("PISA-2022-Mongo-Upload") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0") \
    .config("spark.mongodb.write.connection.uri", "mongodb://127.0.0.1:27017/pisa_database.year2022") \
    .getOrCreate()

# 1. Spark SQL
start_spark = time.time()

df_spark = spark.read.format("mongodb") \
    .option("spark.mongodb.read.connection.uri", "mongodb://127.0.0.1:27017/pisa_database.year2012") \
    .load()

df_spark.createOrReplaceTempView("pisa_2012")
spark_result = spark.sql("SELECT CNT, COUNT(*) as count FROM pisa_2012 GROUP BY CNT").collect()

end_spark = time.time()
spark_time = end_spark - start_spark


# 2. MongoDB Aggregation Framework 
start_mongo = time.time()

pipeline = "[{'$group': {'_id': '$CNT', 'count': {'$sum': 1}}}]"
df_mongo = spark.read.format("mongodb") \
    .option("spark.mongodb.read.connection.uri", "mongodb://127.0.0.1:27017/pisa_database.year2012") \
    .option("aggregation.pipeline", pipeline) \
    .load()

mongo_result = df_mongo.collect()

end_mongo = time.time()
mongo_time = end_mongo - start_mongo

print(f"\n--- SUORITUSKYKYVERTAILU ---")
print(f"Spark SQL suoritusaika: {spark_time:.2f} sekuntia")
print(f"MongoDB Aggregation suoritusaika: {mongo_time:.2f} sekuntia")