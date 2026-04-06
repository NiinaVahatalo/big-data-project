from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, round, lit

# 1. Alustetaan Spark MongoDB-tuella
spark = SparkSession.builder \
    .appName("PISA-Finland-Analysis") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0") \
    .getOrCreate()

uri_2022 = "mongodb://127.0.0.1:27017/pisa_database.year2022"
df_2022 = spark.read.format("mongodb").option("spark.mongodb.read.connection.uri", uri_2022).load()


def get_finland_stats(year):
    uri = f"mongodb://127.0.0.1:27017/pisa_database.year{year}"
    df = spark.read.format("mongodb").option("spark.mongodb.read.connection.uri", uri).load()
    
    fin_df = df.filter(col("CNT") == "FIN")
    
    # Valitaan oikea sarake
    ict_col_name = "ICTRES_SUM"
    
    # Tehdään keskiarvot ja pyöristys suoraan Sparkissa
    stats = fin_df.select(
        round(avg("PV1MATH"), 2).alias("avg_math"),
        round(avg(ict_col_name), 2).alias("avg_ict")
    ).collect()[0]
    
    return stats

try:
    print("\n--- PISA ANALYYSI: SUOMI 2012 vs 2022 ---\n")
    
    stats_2012 = get_finland_stats(2012)
    stats_2022 = get_finland_stats(2022)
    
    # Nyt numerot ovat jo pyöristettyjä, joten voimme tulostaa ne suoraan
    print(f"Vuosi 2012 (Matematiikka): {stats_2012['avg_math']} pistettä")
    print(f"Vuosi 2012 (ICT-resurssit): {stats_2012['avg_ict']} (summamuuttuja)")
    print("-" * 40)
    print(f"Vuosi 2022 (Matematiikka): {stats_2022['avg_math']} pistettä")
    print(f"Vuosi 2022 (ICT-resurssit): {stats_2022['avg_ict']} (summamuuttuja)")
    
    # Lasketaan muutos (tässä käytetään Pythonin omaa matematiikkaa)
    change = stats_2022['avg_math'] - stats_2012['avg_math']
    print(f"\nMuutos matematiikan osaamisessa: {change:.2f} pistettä.")

except Exception as e:
    print(f"Virhe analyysissä: {e}")

finally:
    spark.stop()