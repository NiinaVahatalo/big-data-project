from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

spark = SparkSession.builder.appName("PISA-Filtering-Harmonized").getOrCreate()

# 1. Reading raw Parquet file
raw_df = spark.read.parquet("data/processed/pisa2022.parquet")

# --- Helper functions ---

# Binary Yes/No: 1=Yes -> 1pt, 2=No -> 0pt
def b_yes(c):
    return when(col(c) == 1, 1).otherwise(0)

# Scale 1-8 valid
def s_scale_ST253(c):
    return when(col(c).isin(1, 2, 3, 4, 5, 6, 7, 8), col(c) - 1).otherwise(0)

# Scale 1-4 valid, 5=Don't know=0pt
def s_scale_ST254(c):
    return when(col(c).isin(1, 2, 3, 4), col(c) - 1).otherwise(0)

# --- 2. Proxy ---

df_with_sum = raw_df.withColumn("ICTRES_SUM", 
    # Binary Yes/No
    b_yes("ST250Q02JA") + b_yes("ST250Q03JA") + b_yes("ST250Q04JA") + b_yes("ST250Q05JA") +
    
    # Digital devices
    s_scale_ST253("ST253Q01JA") +
    
    # Devices
    s_scale_ST254("ST254Q01JA") + s_scale_ST254("ST254Q02JA") + s_scale_ST254("ST254Q03JA") + 
    s_scale_ST254("ST254Q04JA") + s_scale_ST254("ST254Q05JA") + s_scale_ST254("ST254Q06JA")
)

# --- 3. Adding columns ---

selected_columns = [
    "CNT", "CNTSTUID", "ST004D01T", "ESCS",
    "PV1MATH", "PV1READ", "PV1SCIE",
    "ICTRES_SUM",
    "ICTRES", "HOMEPOS", "ICTSCH", "ICTAVSCH", "ICTAVHOM",
    "ICTQUAL", "ICTSUBJ", "ICTENQ", "ICTFEED", "ICTOUT", "ICTWKDY", "ICTWKEND",
    "ICTREG", "ICTINFO", "ICTDISTR", "ICTEFFIC",
    "MATHPREF", "MATHEASE", "MATHMOT", "TEACHSUP", "COGACRCO", "COGACMCO",
    "EXPOFA", "EXPO21ST", "MATHEF21", "ANXMAT", "MATHPERS", "PQMIMP", "PQMCAR" 
]

# --- 4. Filtering and adding year column ---

filtered_df = df_with_sum.select(selected_columns) \
    .withColumn("Year", lit(2022)) \
    .dropna(subset=["CNT", "PV1MATH"])

print(f"Original columns: {len(raw_df.columns)}")
print(f"New column count: {len(filtered_df.columns)}")
print(f"Rows remaining: {filtered_df.count()}")

print("Is ICTRES_SUM filled for Finland?")
filtered_df.filter(col("CNT") == "FIN").select("CNT", "PV1MATH", "ICTRES_SUM").show(10)

# --- 5. Saving ---

# New Parquet-file for loading to MongoDB
filtered_df.write.mode("overwrite").parquet("data/processed/pisa2022_clean.parquet")

spark.stop()