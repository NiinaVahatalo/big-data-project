from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

spark = SparkSession.builder.appName("PISA-2012-Final-Harmonization").getOrCreate()

# Loading raw CSV data from PISA 2012
path_2012 = "/home/niina37/big-data-project/data/raw/pisa2012/2012-responses/student-questionnaire-responses.csv"
df = spark.read.csv(path_2012, header=True, inferSchema=True)

# --- HELPER FUNCTIONS ---
def b_yes(c): # Binary Yes: 1=1pt, 2=0pt
    return when(col(c) == 1, 1).otherwise(0)

def s_scale(c): # Subtract 1 from scale (e.g. 1-4 -> 0-3)
    return when(col(c).isNull(), 0).otherwise(col(c) - 1)

# --- 1. CREATING PROXYS ---

# ICTRES (ICT Resources)
df = df.withColumn("ICTRES_SUM", 
    b_yes("ST26Q04") + b_yes("ST26Q05") + b_yes("ST26Q06") + b_yes("ST26Q14") +
    s_scale("ST27Q01") + s_scale("ST27Q02") + s_scale("ST27Q03")
)

# HOMEPOS (Home Possessions)
home_cols = ["ST26Q01", "ST26Q02", "ST26Q03", "ST26Q04", "ST26Q05", "ST26Q06", "ST26Q07", 
             "ST26Q08", "ST26Q09", "ST26Q10", "ST26Q11", "ST26Q12", "ST26Q13", "ST26Q14"]
df = df.withColumn("HOMEPOS_SUM", 
    sum(b_yes(c) for c in home_cols) + s_scale("ST27Q01") + s_scale("ST27Q02") + 
    s_scale("ST27Q03") + s_scale("ST27Q04") + s_scale("ST27Q05") + s_scale("ST28Q01")
)

# ICTAVSCH (School ICT Usage)
sch_cols = ["IC02Q01", "IC02Q02", "IC02Q03", "IC02Q04", "IC02Q05", "IC02Q06", "IC02Q07",
            "IC10Q01", "IC10Q02", "IC10Q03", "IC10Q04", "IC10Q05", "IC10Q06", "IC10Q07", "IC10Q08", "IC10Q09"]
df = df.withColumn("ICTAVSCH_SUM", sum(s_scale(c) for c in sch_cols))

# ICTAVHOM (Home ICT Usage)
hom_cols = ["IC01Q01", "IC01Q02", "IC01Q03", "IC01Q04", "IC01Q05", "IC01Q06", "IC01Q07", 
            "IC01Q08", "IC01Q10", "IC01Q11"]
df = df.withColumn("ICTAVHOM_SUM", sum(s_scale(c) for c in hom_cols))

# --- 2. Creating Final Columns ---

final_columns = [
    # IDs and Background (Standardizing names with 2022)
    col("CNT"),
    col("STIDSTD").alias("CNTSTUID"),
    col("ST04Q01").alias("ST004D01T"),
    "ESCS",
    
    # Results
    "PV1MATH", "PV1READ", "PV1SCIE",
    
    # Indices from data
    "WEALTH", "TIMEINT", "USEMATH", "USESCH",
    
    # Anchored math and teacher variables
    "ANCCLSMAN", "ANCCOGACT", "ANCINSTMOT", "ANCINTMAT", 
    "ANCMATWKETH", "ANCMTSUP", "ANCSCMAT", "ANCSTUDREL", "ANCSUBNORM",
    
    # Created indices
    "ICTRES_SUM", "HOMEPOS_SUM", "ICTAVSCH_SUM", "ICTAVHOM_SUM",
    
    # Time variables (Proxy ICTWKDY/WKEND)
    col("IC06Q01").alias("ICTWKDY"),
    col("IC07Q01").alias("ICTWKEND")
]

# Filter and add year column
df_final = df.select(final_columns).withColumn("Year", lit(2012))

# --- 3. Saving ---
# Saving as Parquet for consistency with 2022 data
df_final.write.mode("overwrite").parquet("data/processed/pisa2012_harmonized.parquet")

print(f"✅ Finished! Columns: {len(df_final.columns)}, Rows: {df_final.count()}")
df_final.show(5)