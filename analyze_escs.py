from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round as spark_round

spark = SparkSession.builder \
    .appName("PISA-ESCS-Analysis") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0") \
    .getOrCreate()

def get_escs_stats(year):
    uri = f"mongodb://127.0.0.1:27017/pisa_database.year{year}"
    df = spark.read.format("mongodb").option("spark.mongodb.read.connection.uri", uri).load()
    
    # Suodatetaan Suomi ja varmistetaan, ettei tyhjiä arvoja lasketa
    fin_df_2012 = df.filter(
        (col("CNT") == "FIN") & 
        (col("ESCS").isNotNull()) & 
        (col("ESCS") > -10) &  # Poistaa -9999
        (col("ESCS") < 10)     # Poistaa 9999, 999 jne.
    )
    
    # Lasketaan korrelaatio ESCS:n ja matematiikan välillä
    correlation = fin_df_2012.stat.corr("ESCS", "PV1MATH")
    avg_escs = fin_df_2012.select(avg("ESCS")).collect()[0][0]
    
    return correlation, avg_escs

try:
    print("\n--- SOSIOEKONOMINEN ANALYYSI (ESCS vs. MATEMATIIKKA) ---")
    
    corr_12, avg_12 = get_escs_stats(2012)
    corr_22, avg_22 = get_escs_stats(2022)
    
    print(f"Vuosi 2012: Korrelaatio = {corr_12:.3f}, Keskiarvo = {avg_12}")
    print(f"Vuosi 2022: Korrelaatio = {corr_22:.3f}, Keskiarvo = {avg_22}")
    
    print("-" * 50)
    diff = corr_22 - corr_12
    print(f"Korrelaation muutos: {diff:+.3f}")

except Exception as e:
    print(f"Virhe analyysissä: {e}")

finally:
    spark.stop()