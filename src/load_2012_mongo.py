from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import pymongo


# 1. Spark-plugin 
spark = SparkSession.builder \
    .appName("PISA-2012-Mongo-Upload") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0") \
    .config("spark.mongodb.write.connection.uri", "mongodb://127.0.0.1:27017/pisa_database.year2012") \
    .getOrCreate()

try:
    # 2. Reading clean Parquet file
    df = spark.read.parquet("data/processed/pisa2012_harmonized.parquet")


    print(f"Loading {df.count()} rows to MongoDB...")

    # 3. Writing to MongoDB
    # Using collection 'year2012'
    df.write.format("mongodb").mode("overwrite").save()

    # Creating index on 'CNT' for faster queries
    client = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
    db = client.pisa_database
    db.year2012.create_index([("CNT", 1)])

    print("\n✅ READY! PISA 2012 data is now in MongoDB (pisa_database.year2012).")

except Exception as e:
    print(f"\n❌ ERROR: {e}")

finally:
    spark.stop()