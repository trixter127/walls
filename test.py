import os
import pyarrow.parquet as pq
import pandas as pd

input_folder = '/path/to/parquet_files/'
output_folder = '/path/to/csv_files/'

for filename in os.listdir(input_folder):
    if filename.endswith('.parquet'):
        file_path = os.path.join(input_folder, filename)
        output_file = os.path.splitext(filename)[0] + '.csv'
        output_path = os.path.join(output_folder, output_file)

        table = pq.read_table(file_path)
        df = table.to_pandas()
        df.to_csv(output_path, index=False)
