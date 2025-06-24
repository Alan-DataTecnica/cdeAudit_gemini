# purge_icd9_cdes.py
# Purpose: A standalone utility script to permanently remove all CDEs that
# reference "ICD9" from the primary processed catalog.

import os
import pandas as pd
import shutil

# --- CONFIGURATION ---
CDE_CATALOG_PATH = os.path.join('outputs', 'stage_1', 'cde_catalog_processed.csv')
# The substring to search for (case-insensitive)
SUBSTRING_TO_DELETE = "ICD9"
# --- NEW: Define all columns to search ---
COLUMNS_TO_SEARCH = ['title', 'variable_name', 'short_description']

def purge_icd9():
    """
    Identifies and deletes rows containing the specified substring from the CDE catalog.
    Creates a backup of the original file before making any changes.
    """
    print("--- Starting ICD9 CDE Purge Utility ---")

    # 1. Check if the target file exists
    if not os.path.exists(CDE_CATALOG_PATH):
        print(f"Error: CDE Catalog not found at '{CDE_CATALOG_PATH}'. Aborting.")
        return

    # 2. Create a backup
    backup_path = CDE_CATALOG_PATH + ".bak"
    try:
        shutil.copy2(CDE_CATALOG_PATH, backup_path)
        print(f"Successfully created a backup of the original file at: {backup_path}")
    except Exception as e:
        print(f"Error: Could not create backup file. Aborting to prevent data loss. Details: {e}")
        return

    # 3. Load the data
    print(f"Loading data from '{CDE_CATALOG_PATH}'...")
    try:
        df = pd.read_csv(CDE_CATALOG_PATH, dtype={'ID': str}, low_memory=False)
        initial_row_count = len(df)
        print(f"Successfully loaded {initial_row_count:,} CDEs.")
    except Exception as e:
        print(f"Error: Could not read the CSV file. Details: {e}")
        return

    # 4. Identify and delete rows
    # --- FIX: Search across multiple specified columns ---
    print(f"Searching for '{SUBSTRING_TO_DELETE}' in columns: {', '.join(COLUMNS_TO_SEARCH)}...")
    
    # Create a boolean mask. A row will be marked True if the substring is found in ANY of the specified columns.
    mask = df[COLUMNS_TO_SEARCH].apply(
        lambda col: col.str.contains(SUBSTRING_TO_DELETE, case=False, na=False)
    ).any(axis=1)

    rows_to_delete = df[mask]
    num_deleted = len(rows_to_delete)

    if num_deleted == 0:
        print("No CDEs containing 'ICD9' were found. No changes made.")
    else:
        print(f"Found {num_deleted} CDEs containing '{SUBSTRING_TO_DELETE}'. Deleting them now...")
        # Keep rows that do NOT match the condition (where the mask is False)
        df_cleaned = df[~mask]
        
        # 5. Save the cleaned data, overwriting the original file
        try:
            df_cleaned.to_csv(CDE_CATALOG_PATH, index=False)
            print(f"Successfully saved the cleaned catalog. New row count: {len(df_cleaned):,}.")
        except Exception as e:
            print(f"Error: Could not save the cleaned file. Your original data is safe in the backup. Details: {e}")

    print("--- Purge Utility Finished ---")


if __name__ == "__main__":
    purge_icd9()

