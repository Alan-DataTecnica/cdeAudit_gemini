# add_new_cdes.py
# Purpose: A standalone utility script to read a CSV of new CDEs,
# assign them unique, non-conflicting IDs, and append them to the
# main CDE catalog.

import os
import pandas as pd
import logging
import csv

# --- CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Input Paths
# This should be the most up-to-date, definitive version of your catalog.
MAIN_CATALOG_PATH = os.path.join('outputs', 'stage_4', 'cde_catalog_final.csv')
# Path to the CSV file containing the new CDEs to add.
NEW_CDES_PATH = 'cdeCatalogs/GovernanceCDEs - Sheet1.csv' 


def add_new_cdes():
    """
    Loads new CDEs, assigns them unique IDs, and appends them to the main catalog.
    """
    print("--- Starting 'Add New CDEs' Utility ---")

    # 1. Validate that input files exist
    if not os.path.exists(MAIN_CATALOG_PATH):
        logging.error(f"Main catalog not found at: '{MAIN_CATALOG_PATH}'. Aborting.")
        return
    if not os.path.exists(NEW_CDES_PATH):
        logging.error(f"New CDEs file not found at: '{NEW_CDES_PATH}'. Aborting.")
        return

    # 2. Load the main catalog to find the highest existing ID
    try:
        logging.info(f"Loading main catalog from '{MAIN_CATALOG_PATH}' to determine starting ID...")
        main_df = pd.read_csv(MAIN_CATALOG_PATH, dtype={'ID': str}, low_memory=False)
        
        # Safely find the maximum numeric ID
        numeric_ids = pd.to_numeric(main_df['ID'], errors='coerce').dropna()
        max_id = int(numeric_ids.max()) if not numeric_ids.empty else -1
        
        logging.info(f"Successfully loaded {len(main_df)} existing CDEs. Highest current ID is {max_id}.")
        
    except Exception as e:
        logging.error(f"Could not read the main catalog file. Error: {e}")
        return

    # 3. Load the new CDEs to be added
    try:
        logging.info(f"Loading new CDEs from '{NEW_CDES_PATH}'...")
        new_cdes_df = pd.read_csv(NEW_CDES_PATH)
        logging.info(f"Found {len(new_cdes_df)} new CDEs to add.")
    except Exception as e:
        logging.error(f"Could not read the new CDEs file. Error: {e}")
        return

    # 4. Assign new, unique IDs
    logging.info(f"Assigning new IDs starting from {max_id + 1}...")
    new_cdes_df['ID'] = range(max_id + 1, max_id + 1 + len(new_cdes_df))
    
    # 5. Conform new CDEs to the main catalog's schema
    # Ensure all columns from the main catalog exist in the new CDEs DataFrame
    for col in main_df.columns:
        if col not in new_cdes_df.columns:
            new_cdes_df[col] = pd.NA # Add missing columns with null values

    # Ensure the column order is the same
    new_cdes_df = new_cdes_df[main_df.columns]

    # 6. Append the new records to the main dataframe
    logging.info("Appending new CDEs to the main catalog...")
    combined_df = pd.concat([main_df, new_cdes_df], ignore_index=True)

    # 7. Save the updated catalog, overwriting the original file
    try:
        logging.info(f"Saving updated catalog with {len(combined_df)} total records back to '{MAIN_CATALOG_PATH}'...")
        # Use robust quoting to prevent file corruption
        combined_df.to_csv(MAIN_CATALOG_PATH, index=False, quoting=csv.QUOTE_ALL)
        logging.info("✅ Success! The main catalog has been updated.")
    except Exception as e:
        logging.error(f"Failed to save the updated catalog. Error: {e}")

    print("--- Utility Finished ---")


if __name__ == "__main__":
    add_new_cdes()
