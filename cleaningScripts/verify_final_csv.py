# verify_final_csv.py
# Purpose: A standalone utility script to programmatically read and verify the
# integrity of the final exported CSV file, ensuring it is not corrupted.

import os
import pandas as pd
import csv
import logging

# --- CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Path to the final output file from Stage 4
FINAL_CATALOG_PATH = os.path.join('outputs', 'stage_4', 'cde_catalog_final.csv')


def verify_csv():
    """
    Attempts to read the final CSV using strict quoting rules to verify its structure.
    """
    print("--- Starting Final CSV Verification Utility ---")

    # 1. Check if the target file exists
    if not os.path.exists(FINAL_CATALOG_PATH):
        logging.error(f"Error: Final catalog not found at '{FINAL_CATALOG_PATH}'.")
        logging.error("Please run the consolidation script to generate it.")
        return

    # 2. Attempt to read the CSV file
    logging.info(f"Attempting to read and parse '{FINAL_CATALOG_PATH}'...")
    try:
        # We use the same quoting rule that was used to create the file.
        # If this read operation succeeds, the file structure is valid.
        df = pd.read_csv(
            FINAL_CATALOG_PATH,
            dtype=str,  # Read all columns as strings to avoid type inference issues
            quoting=csv.QUOTE_ALL
        )
        
        # 3. Report Success
        num_rows, num_cols = df.shape
        logging.info("✅ SUCCESS: The file was parsed successfully.")
        logging.info(f"The file appears to be correctly formatted and is not corrupted.")
        logging.info(f"Dimensions: {num_rows:,} rows x {num_cols:,} columns.")
        logging.info("You can view a sample of the loaded data below:")
        
        # Display the head and tail to show it loaded correctly
        print("\n--- Top 5 Rows ---")
        print(df.head())
        print("\n--- Bottom 5 Rows ---")
        print(df.tail())

    except pd.errors.ParserError as e:
        # 4. Report Failure
        logging.error("❌ FAILURE: The file could not be parsed and is likely corrupted.")
        logging.error(f"The display issue is not just a quirk; the file has structural errors.")
        logging.error(f"Parser Error Details: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred. Details: {e}")

    print("\n--- Verification Utility Finished ---")


if __name__ == "__main__":
    verify_csv()
