import pandas as pd
import os

def get_sales_from_csv():
    # Define Data folder path
    input_file = 'data.csv'
    data_folder = os.path.join(os.getcwd(), "Data")

    # Define file path
    file_path = os.path.join(data_folder, input_file)

    # Check if the file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File '{input_file}' not found in the 'Data' folder.")

    # Read the CSV file
    df = pd.read_csv(file_path)

    return df