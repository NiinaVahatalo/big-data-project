from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("FinlandAlternative").getOrCreate()

# Luetaan se alkuperäinen 1278 sarakkeen tiedosto
raw_df = spark.read.parquet("data/processed/pisa2022.parquet")
fin_raw = raw_df.filter(col("CNT") == "FIN")

# Etsitään sarakkeita, joissa on "Computer" tai "Internet" ja joissa ON dataa Suomelle
# PISA 2022 koodit kodin tavaroille alkavat usein ST250Q
test_cols = ["ST250Q01JA", "ST250Q02JA", "ST250Q03JA", "HOMEPOS", "ESCS"]

print("\n--- Vaihtoehtoiset ICT-muuttujat Suomelle (2022) ---")
for c in test_cols:
    if c in fin_raw.columns:
        count = fin_raw.filter(col(c).isNotNull()).count()
        print(f"Sarake {c:15}: {count} täytettyä riviä Suomessa")

spark.stop()