# inventory_headers.py
# Purpose: A utility script to scan a directory for CSV files and compile
# a comprehensive list of all unique column headers found.

import pandas as pd
import os
from tqdm import tqdm

# --- Configuration ---
# The directory to scan for CSV files.
SOURCE_DIRECTORY = 'cdeCatalogs/'

# The name of the output report file.
OUTPUT_FILE = 'all_headers_report.csv'

def find_all_headers():
    """
    Scans the SOURCE_DIRECTORY for .csv files and extracts all unique headers.
    """
    if not os.path.isdir(SOURCE_DIRECTORY):
        print(f"Error: Source directory '{SOURCE_DIRECTORY}' not found.")
        return

    all_headers = set()
    csv_files_to_scan = []

    # First, find all CSV files in the directory and its subdirectories
    for root, _, files in os.walk(SOURCE_DIRECTORY):
        for file in files:
            if file.lower().endswith('.csv'):
                csv_files_to_scan.append(os.path.join(root, file))
    
    if not csv_files_to_scan:
        print(f"No CSV files found in '{SOURCE_DIRECTORY}'.")
        return

    print(f"Found {len(csv_files_to_scan)} CSV files. Scanning headers...")

    # Now, read only the header of each file to get column names
    for filepath in tqdm(csv_files_to_scan, desc="Scanning files"):
        try:
            # Optimization: nrows=0 reads only the header, not the whole file.
            # This is very fast and memory-efficient.
            df = pd.read_csv(filepath, nrows=0, encoding='utf-8', on_bad_lines='skip')
            all_headers.update(df.columns)
        except Exception as e:
            print(f"\nCould not read headers from {filepath}. Error: {e}")

    if not all_headers:
        print("No headers were successfully extracted.")
        return

    # Convert the set of headers to a DataFrame and save to a new CSV
    headers_df = pd.DataFrame(sorted(list(all_headers)), columns=['header_name'])
    headers_df.to_csv(OUTPUT_FILE, index=False)

    print(f"\nSuccessfully generated header report: {OUTPUT_FILE}")
    print(f"Found a total of {len(all_headers)} unique headers.")

if __name__ == "__main__":
    find_all_headers()