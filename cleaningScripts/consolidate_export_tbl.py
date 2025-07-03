# consolidate_and_export.py
# Purpose: A final, standalone script to create the definitive CDE catalog.
# It reads directly from the SQLite database, exports the data cleanly,
# filters it to match the working set of CDEs, and then applies all 
# decisions from the review tool to produce the final output.

import os
import pandas as pd
import json
import logging
import sqlite3
import csv

# --- CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Input paths
DATABASE_PATH = os.path.join('cdeCatalogs', '20250603_2030_cde.sqlite')
TABLE_NAME = "CDE_Dictionary_Condensed"
REVIEW_STATE_PATH = os.path.join('stage3_adjudication_output', 'review_progress.json')
# --- NEW: Path to the working catalog to define which rows to keep ---
WORKING_CATALOG_PATH = os.path.join('outputs', 'stage_1', 'cde_catalog_processed.csv')


# Output path
OUTPUT_DIR = 'outputs/stage_4'
FINAL_CATALOG_FILENAME = 'cde_catalog_final.csv'
os.makedirs(OUTPUT_DIR, exist_ok=True)
FINAL_CATALOG_PATH = os.path.join(OUTPUT_DIR, FINAL_CATALOG_FILENAME)


# --- DATA LOADING ---
def load_and_filter_data_from_db():
    """
    Loads data from the SQLite DB and filters it to only include CDEs
    that exist in the current working catalog.
    """
    # 1. Get the list of valid IDs from the working catalog
    logging.info(f"Loading working CDE list from: {WORKING_CATALOG_PATH}")
    if not os.path.exists(WORKING_CATALOG_PATH):
        logging.error(f"Working catalog not found at '{WORKING_CATALOG_PATH}'. Cannot determine which CDEs to keep. Aborting.")
        return None
    try:
        working_df = pd.read_csv(WORKING_CATALOG_PATH, dtype={'ID': str}, usecols=['ID'])
        valid_cde_ids = set(working_df['ID'].unique())
        logging.info(f"Found {len(valid_cde_ids):,} unique CDE IDs in the working catalog.")
    except Exception as e:
        logging.error(f"Could not read the working catalog CSV. Error: {e}")
        return None

    # 2. Load the full, clean data from the database
    logging.info(f"Connecting to source SQLite database at: {DATABASE_PATH}")
    if not os.path.exists(DATABASE_PATH):
        logging.error("Database file not found. Aborting.")
        return None
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        query = f"SELECT * FROM {TABLE_NAME}"
        db_df = pd.read_sql_query(query, conn)
        conn.close()
        db_df['ID'] = db_df['ID'].astype(str)
        logging.info(f"Successfully loaded {len(db_df)} records from the database.")
    except Exception as e:
        logging.error(f"Failed to load data from SQLite database. Error: {e}")
        return None

    # 3. Filter the database records to match the working set
    logging.info("Filtering database records to match the working set of CDEs...")
    filtered_df = db_df[db_df['ID'].isin(valid_cde_ids)]
    logging.info(f"Retained {len(filtered_df)} records from the database that exist in the working catalog.")
    
    return filtered_df

def load_review_state():
    """Loads the review decisions."""
    logging.info(f"Loading review decisions from: {REVIEW_STATE_PATH}")
    if not os.path.exists(REVIEW_STATE_PATH):
        logging.warning("Review state file not found. No changes will be applied.")
        return {}
    with open(REVIEW_STATE_PATH, 'r') as f:
        return json.load(f)


# --- CORE LOGIC ---
def apply_changes(df: pd.DataFrame, state: dict) -> pd.DataFrame:
    """Applies all reviewed decisions to the DataFrame."""
    if not state:
        logging.warning("Review state is empty. No changes will be applied.")
        return df

    final_df = df.copy()

    # 1. Handle Deletions First
    deleted_cde_ids = [k.split('|')[0] for k, v in state.items() if '__CDE_STATUS__' in k and v.get('status') == 'deleted']
    if deleted_cde_ids:
        logging.info(f"Deleting {len(deleted_cde_ids)} CDEs marked for deletion...")
        final_df = final_df[~final_df['ID'].isin(deleted_cde_ids)]
        logging.info(f"Deletion complete. New CDE count: {len(final_df)}")

    # 2. Handle Accepted Field Changes
    final_df.set_index('ID', inplace=True)
    
    accepted_changes = {k: v for k, v in state.items() if v.get('status') == 'accepted'}
    logging.info(f"Found {len(accepted_changes)} accepted suggestions to apply.")

    for suggestion_key, details in accepted_changes.items():
        cde_id, field = suggestion_key.split('|')
        if cde_id not in final_df.index:
            continue
            
        suggested_value = details.get('suggestion')

        if field == 'collections':
            original_collections_str = final_df.loc[cde_id, field]
            original_collections = set(str(original_collections_str).split('|')) if pd.notna(original_collections_str) else set()
            suggested_collections = set(str(suggested_value).split('|')) if pd.notna(suggested_value) else set()
            combined_collections = original_collections.union(suggested_collections)
            combined_collections.discard('')
            final_value = '|'.join(sorted(list(combined_collections)))
            final_df.loc[cde_id, field] = final_value
        else:
            final_df.loc[cde_id, field] = suggested_value
            
    final_df.reset_index(inplace=True)
    logging.info("All accepted changes have been applied.")
    return final_df


# --- MAIN EXECUTION ---
def main():
    """Main function to consolidate and export the final CDE catalog."""
    logging.info("--- Starting Final Consolidation and Export Process ---")

    cde_df = load_and_filter_data_from_db()
    if cde_df is None:
        return
        
    review_state = load_review_state()
    
    final_df = apply_changes(cde_df, review_state)
    
    try:
        # Use csv.QUOTE_ALL to ensure complex strings are handled correctly
        final_df.to_csv(FINAL_CATALOG_PATH, index=False, quoting=csv.QUOTE_ALL)
        logging.info(f"Successfully generated final CDE catalog at: {FINAL_CATALOG_PATH}")
        logging.info(f"The final catalog contains {len(final_df)} CDEs.")
    except Exception as e:
        logging.error(f"Failed to save the final CSV file. Error: {e}")

    logging.info("--- Finalization Process Complete ---")

if __name__ == "__main__":
    main()

