import pyreadstat
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

input_file = "data/raw/CY08MSP_STU_QQQ.SAV" 
output_file = "data/processed/pisa2022.parquet"
os.makedirs("data/processed", exist_ok=True)

print("Beginning transformation with PyArrow engine...")

try:
    # 1. Reading metadata to get row and column counts for progress tracking
    _, meta = pyreadstat.read_sav(input_file, metadataonly=True)
    print(f"File contains {meta.number_rows} rows and {meta.number_columns} columns.")

    chunk_size = 50000 
    offset = 0
    writer = None

    while offset < meta.number_rows:
        # Reading a chunk of data
        df, _ = pyreadstat.read_sav(input_file, row_limit=chunk_size, row_offset=offset)
        
        if df.empty:
            break

        # Fixing object columns (e.g., 'CNT'), so they don't break the writing process
        for col in df.select_dtypes(['object']).columns:
            df[col] = df[col].astype(str)

        # Converting Pandas DataFrame to PyArrow table
        table = pa.Table.from_pandas(df)

        # Writing the table to the Parquet file
        if writer is None:
            writer = pq.ParquetWriter(output_file, table.schema)
        

        writer.write_table(table)
        
        offset += chunk_size
        print(f"Processed {min(offset, meta.number_rows)} / {meta.number_rows} rows...")

    if writer:
        writer.close()

    print(f"\n✅ Finished! File saved to: {output_file}")

except Exception as e:
    print(f"\n❌ ERROR: {e}")
