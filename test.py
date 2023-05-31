import os
import pyarrow.parquet as pq
import pandas as pd

# Specify the path to the level1 folder
level1_folder = '/path/to/level1/'

# Iterate through each level2 folder
for level2_folder in os.listdir(level1_folder):
    level2_folder_path = os.path.join(level1_folder, level2_folder)
    
    # Check if it's a directory
    if os.path.isdir(level2_folder_path):
        # Create an empty DataFrame to hold the combined data
        combined_df = pd.DataFrame()
        
        # Iterate through each Parquet file in the level2 folder
        for file in os.listdir(level2_folder_path):
            if file.endswith('.parquet'):
                file_path = os.path.join(level2_folder_path, file)
                
                # Read the Parquet file into a PyArrow table
                table = pq.read_table(file_path)
                
                # Convert the table to a Pandas DataFrame
                df = table.to_pandas()
                
                # Append the DataFrame to the combined DataFrame
                combined_df = combined_df.append(df)
        
        # Generate the output CSV file path
        output_csv_path = os.path.join(level1_folder, f'{level2_folder}.csv')
        
        # Save the combined DataFrame as a CSV file
        combined_df.to_csv(output_csv_path, index=False)
