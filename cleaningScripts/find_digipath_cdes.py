# find_digipath_matches.py
# Purpose: A standalone utility to compare a new set of CDEs against the main
# catalog and generate a report of all existing matches.

import os
import pandas as pd
import logging
import csv
from tqdm import tqdm

# --- CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Input Paths
MAIN_CATALOG_PATH = os.path.join('outputs', 'final_enriched', 'cde_catalog_enriched.csv')
NEW_CDES_PATH = os.path.join('cdeCatalogs', 'digipathCDEs.csv')

# Output Path
OUTPUT_DIR = 'outputs/reports'
MATCH_REPORT_FILENAME = 'digipath_matches_report.csv'
os.makedirs(OUTPUT_DIR, exist_ok=True)
MATCH_REPORT_PATH = os.path.join(OUTPUT_DIR, MATCH_REPORT_FILENAME)

# --- MAIN LOGIC ---
def find_matches():
    """
    Main function to load both catalogs and find matching CDEs.
    """
    logging.info("--- Starting DigiPath CDE Match Finder ---")

    # 1. Load Data Sources
    logging.info("Loading data sources...")
    try:
        main_df = pd.read_csv(MAIN_CATALOG_PATH, dtype=str, low_memory=False)
        main_df.fillna('', inplace=True)
        logging.info(f"Loaded {len(main_df)} CDEs from the main catalog.")

        new_cdes_df = pd.read_csv(NEW_CDES_PATH, dtype=str, low_memory=False)
        new_cdes_df.fillna('', inplace=True)
        logging.info(f"Loaded {len(new_cdes_df)} CDEs from the DigiPath catalog.")
        
        # --- FIX: Add a pre-flight check to validate headers ---
        required_cols = {'title', 'variable_name'}
        if not required_cols.issubset(new_cdes_df.columns):
            logging.error(f"Input file '{NEW_CDES_PATH}' is missing required columns.")
            logging.error(f"Expected columns: {list(required_cols)}. Found: {list(new_cdes_df.columns)}")
            logging.error("Please correct the input file's format. Aborting.")
            return

    except FileNotFoundError as e:
        logging.error(f"File not found: {e}. Please ensure file paths are correct.")
        return
    except Exception as e:
        logging.error(f"Failed to load initial data. Error: {e}"); return

    # 2. Find Matches
    logging.info("Searching for matches based on 'title' and 'variable_name'...")
    
    found_matches = []

    for _, new_cde_row in tqdm(new_cdes_df.iterrows(), total=len(new_cdes_df), desc="Finding Matches"):
        new_title = new_cde_row['title'].strip().lower()
        new_var_name = new_cde_row['variable_name'].strip().lower()

        # Skip if both identifiers are empty for the new CDE
        if not new_title and not new_var_name:
            continue

        title_match_mask = main_df['title'].str.strip().str.lower() == new_title if new_title else pd.Series([False] * len(main_df))
        var_name_match_mask = main_df['variable_name'].str.strip().str.lower() == new_var_name if new_var_name else pd.Series([False] * len(main_df))
        
        combined_mask = title_match_mask | var_name_match_mask
        
        matching_rows = main_df[combined_mask]

        if not matching_rows.empty:
            for _, match_row in matching_rows.iterrows():
                # Determine match type more robustly
                match_type = []
                if new_title and title_match_mask[match_row.name]:
                    match_type.append('title')
                if new_var_name and var_name_match_mask[match_row.name]:
                    match_type.append('variable_name')
                
                found_matches.append({
                    'new_cde_title': new_cde_row['title'],
                    'new_cde_variable_name': new_cde_row['variable_name'],
                    'match_type': ' & '.join(match_type),
                    'existing_cde_id': match_row['ID'],
                    'existing_cde_title': match_row['title'],
                    'existing_cde_variable_name': match_row['variable_name']
                })

    # 3. Generate and Save the Report
    if not found_matches:
        logging.info("No matches found between the DigiPath CDEs and the main catalog.")
    else:
        logging.info(f"Found {len(found_matches)} total matches. Generating report...")
        report_df = pd.DataFrame(found_matches)
        
        try:
            report_df.to_csv(MATCH_REPORT_PATH, index=False, quoting=csv.QUOTE_ALL)
            logging.info(f"✅ Success! Match report saved to: {MATCH_REPORT_PATH}")
        except Exception as e:
            logging.error(f"Failed to save the match report. Error: {e}")

    logging.info("--- Match Finding Process Complete ---")


if __name__ == "__main__":
    find_matches()
