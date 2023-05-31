from pathlib import Path
import pyarrow.parquet as pq
import pandas as pd

# Specify the path to the level1 folder
level1_folder = Path('/path/to/level1/')

# Iterate through each level2 folder
for level2_folder in level1_folder.iterdir():
    if level2_folder.is_dir():
        dfs = []

        # Iterate through each Parquet file in the level2 folder
        for file_path in level2_folder.glob('*.parquet'):
            # Read the Parquet file into a PyArrow table
            table = pq.read_table(file_path)

            # Convert the table to a Pandas DataFrame
            df = table.to_pandas()

            # Add the DataFrame to the list
            dfs.append(df)

        # Concatenate all DataFrames in the list
        combined_df = pd.concat(dfs, ignore_index=True)

        # Generate the output CSV file path
        output_csv_path = level1_folder / f'{level2_folder.stem}.csv'

        # Save the combined DataFrame as a CSV file
        combined_df.to_csv(output_csv_path, index=False)
