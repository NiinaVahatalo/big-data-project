from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

spark = SparkSession.builder.appName("PISA-2012-Final-Harmonization").getOrCreate()

# 1. Ladataan data
path_2012 = "/home/niina37/big-data-project/data/raw/pisa2012/2012-responses/student-questionnaire-responses.csv"
df = spark.read.csv(path_2012, header=True, inferSchema=True)

# --- APUFUNKTIOT ---
def b_yes(c): 
    return when(col(c) == 1, 1).otherwise(0)

def s_scale(c): 
    return when(col(c).isNull(), 0).when(col(c) > 10, 0).otherwise(col(c) - 1)

def p_home(c):
    return when(col(c).isin(1, 2), 1).otherwise(0)

def p_time(c):
    return when(col(c).between(1, 7), col(c) - 1).otherwise(0)

def p_act(c):
    return when(col(c).between(1, 5), col(c) - 1).otherwise(0)

# --- 2. LUODAAN INDEKSIT (PROXIES) ---

# ICTRES_SUM: Fyysiset resurssit (ST26/ST27 muuttujat)
df = df.withColumn("ICTRES_SUM", 
    b_yes("ST26Q04") + b_yes("ST26Q05") + b_yes("ST26Q06") + b_yes("ST26Q14") +
    s_scale("ST27Q01") + s_scale("ST27Q02") + s_scale("ST27Q03")
)

# HOMEPOS_SUM: Kodin yleisvarallisuus
home_cols = ["ST26Q01", "ST26Q02", "ST26Q03", "ST26Q04", "ST26Q05", "ST26Q06", "ST26Q07", 
             "ST26Q08", "ST26Q09", "ST26Q10", "ST26Q11", "ST26Q12", "ST26Q13", "ST26Q14"]
df = df.withColumn("HOMEPOS_SUM", 
    sum(b_yes(c) for c in home_cols) + s_scale("ST27Q01") + s_scale("ST27Q02") + 
    s_scale("ST27Q03") + s_scale("ST27Q04") + s_scale("ST27Q05") + s_scale("ST28Q01")
)

# ICTAVSCH_SUM: Kouluympäristön ICT
sch_cols = ["IC02Q01", "IC02Q02", "IC02Q03", "IC02Q04", "IC02Q05", "IC02Q06", "IC02Q07",
            "IC10Q01", "IC10Q02", "IC10Q03", "IC10Q04", "IC10Q05", "IC10Q06", "IC10Q07", "IC10Q08", "IC10Q09"]
df = df.withColumn("ICTAVSCH_SUM", sum(s_scale(c) for c in sch_cols))

# ICTAVHOM_SUM: Laaja ICT-käyttö ja saatavuus kotona (Uudet 30 muuttujaasi)
df = df.withColumn("ICTAVHOM_SUM", 
    # At Home (IC01-sarja)
    p_home("IC01Q01") + p_home("IC01Q02") + p_home("IC01Q03") + p_home("IC01Q04") + 
    p_home("IC01Q05") + p_home("IC01Q06") + p_home("IC01Q07") + p_home("IC01Q08") + 
    p_home("IC01Q10") + p_home("IC01Q11") + 
    
    # Internet käyttöaika
    p_time("IC06Q01") + p_time("IC07Q01") +
    
    # Vapaa-ajan aktiviteetit (IC08)
    p_act("IC08Q01") + p_act("IC08Q02") + p_act("IC08Q03") + p_act("IC08Q04") + 
    p_act("IC08Q05") + p_act("IC08Q06") + p_act("IC08Q07") + p_act("IC08Q08") + 
    p_act("IC08Q09") + p_act("IC08Q11") +
    
    # Kouluun liittyvä käyttö kotona (IC09)
    p_act("IC09Q01") + p_act("IC09Q02") + p_act("IC09Q03") + p_act("IC09Q04") + 
    p_act("IC09Q05") + p_act("IC09Q06") + p_act("IC09Q07")
)

# --- 3. SARAKKEIDEN LOPULLINEN VALINTA ---

# Standardoidaan nimet vuoden 2022 datan kanssa
final_columns = [
    col("CNT"),
    col("STIDSTD").alias("CNTSTUID"),
    col("ST04Q01").alias("ST004D01T"),
    col("ESCS"),
    col("PV1MATH"), col("PV1READ"), col("PV1SCIE"),
    col("WEALTH"), col("TIMEINT"), col("USEMATH"), col("USESCH"),
    
    # Anchored variables
    "ANCCLSMAN", "ANCCOGACT", "ANCINSTMOT", "ANCINTMAT", 
    "ANCMATWKETH", "ANCMTSUP", "ANCSCMAT", "ANCSTUDREL", "ANCSUBNORM",
    
    # Luodut summat
    "ICTRES_SUM", "HOMEPOS_SUM", "ICTAVSCH_SUM", "ICTAVHOM_SUM",
    
    # Aikamuuttujat (Säilytetään alkuperäiset nimet ja luodaan aliakset vertailuun)
    col("IC06Q01").alias("ICTWKDY"),
    col("IC07Q01").alias("ICTWKEND")
]

# Lopullinen suodatus ja vuoden lisäys
df_final = df.select(final_columns).withColumn("Year", lit(2012))

# --- 4. TALLENNUS ---
df_final.write.mode("overwrite").parquet("data/processed/pisa2012_harmonized.parquet")

print(f"✅ Valmis! Sarakkeita: {len(df_final.columns)}, Rivejä: {df_final.count()}")
df_final.select("CNT", "ICTAVHOM_SUM", "ICTWKDY").show(5)