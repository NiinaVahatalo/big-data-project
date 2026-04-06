from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, round, lit

# 1. Alustetaan Spark MongoDB-tuella
spark = SparkSession.builder \
    .appName("PISA-Finland-Analysis") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0") \
    .getOrCreate()

uri_2022 = "mongodb://127.0.0.1:27017/pisa_database.year2022"
df_2022 = spark.read.format("mongodb").option("spark.mongodb.read.connection.uri", uri_2022).load()

def get_correlation(year):
    uri = f"mongodb://127.0.0.1:27017/pisa_database.year{year}"
    df = spark.read.format("mongodb").option("spark.mongodb.read.connection.uri", uri).load()
    
    # Suodatetaan Suomi ja poistetaan tyhjät, jotta korrelaatio on tarkka
    fin_df = df.filter((col("CNT") == "FIN") & 
                       (col("ICTRES_SUM").isNotNull()) & 
                       (col("PV1MATH").isNotNull()))
    
    # Lasketaan korrelaatio ICT-resurssien ja matematiikan välillä
    correlation = fin_df.stat.corr("ICTRES_SUM", "PV1MATH")
    return correlation

corr_2012 = get_correlation(2012)
corr_2022 = get_correlation(2022)

print(f"\n--- KORRELAATIOANALYYSI (ICT vs. MATEMATIIKKA) ---")
print(f"Korrelaatio vuonna 2012: {corr_2012:.3f}")
print(f"Korrelaatio vuonna 2022: {corr_2022:.3f}")