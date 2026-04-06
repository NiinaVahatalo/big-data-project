from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round as spark_round

spark = SparkSession.builder \
    .appName("PISA-Global-Analysis") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0") \
    .getOrCreate()

def get_global_stats(year):
    uri = f"mongodb://127.0.0.1:27017/pisa_database.year{year}"
    
    # Valitaan sarakkeet JO LATAUSVAIHEESSA
    df = spark.read.format("mongodb") \
        .option("spark.mongodb.read.connection.uri", uri) \
        .load() \
        .select("PV1MATH", "ICTRES_SUM", "ESCS", "CNT") # Lataa vain nämä!

def get_global_stats(year):
    uri = f"mongodb://127.0.0.1:27017/pisa_database.year{year}"
    df = spark.read.format("mongodb").option("spark.mongodb.read.connection.uri", uri).load()
    
    # PUHDISTUS: Poistetaan sentinel-arvot (esim. -9999) ja tyhjät
    # Käytetään samaa -10...10 suodatinta ESCS:lle kuin Suomen kohdalla
    clean_df = df.filter(
        (col("PV1MATH").isNotNull()) & 
        (col("ICTRES_SUM").isNotNull()) &
        (col("ESCS").isNotNull()) &
        (col("ESCS") > -10) & (col("ESCS") < 10)
    )
    
    # Lasketaan globaalit keskiarvot
    averages = clean_df.select(
        spark_round(avg("PV1MATH"), 2).alias("avg_math"),
        spark_round(avg("ICTRES_SUM"), 2).alias("avg_ict")
    ).collect()[0]
    
    # Lasketaan globaalit korrelaatiot
    corr_ict = clean_df.stat.corr("ICTRES_SUM", "PV1MATH")
    corr_escs = clean_df.stat.corr("ESCS", "PV1MATH")
    
    return {
        "math": averages["avg_math"],
        "ict": averages["avg_ict"],
        "corr_ict": corr_ict,
        "corr_escs": corr_escs,
        "count": clean_df.count()
    }

try:
    print("\n" + "="*60)
    print("GLOBAALI PISA-VERTAILU: 2012 vs 2022 (KAIKKI MAAT)")
    print("="*60)
    
    stats_12 = get_global_stats(2012)
    stats_22 = get_global_stats(2022)
    
    # Tulostetaan taulukko
    template = "{:<30} {:<15} {:<15}"
    print(template.format("Mittari", "Vuosi 2012", "Vuosi 2022"))
    print("-" * 60)
    print(template.format("Matematiikka (ka)", stats_12['math'], stats_22['math']))
    print(template.format("ICT-resurssit (ka)", stats_12['ict'], stats_22['ict']))
    print(template.format("ICT-Matikka Korrelaatio", f"{stats_12['corr_ict']:.3f}", f"{stats_22['corr_ict']:.3f}"))
    print(template.format("ESCS-Matikka Korrelaatio", f"{stats_12['corr_escs']:.3f}", f"{stats_22['corr_escs']:.3f}"))
    print("-" * 60)
    print(f"Data-pisteitä yhteensä: 2012: {stats_12['count']} | 2022: {stats_22['count']}")

except Exception as e:
    print(f"Virhe analyysissä: {e}")

finally:
    spark.stop()