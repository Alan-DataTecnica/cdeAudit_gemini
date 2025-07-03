# merge_and_update_catalog.py
# Purpose: A final, standalone script to merge the DigiPath CDEs into the
# main catalog, intelligently update matching records, and perform a final
# deduplication based on the matches found.

import os
import pandas as pd
import logging
import csv
from tqdm import tqdm

# --- CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Input Paths
MAIN_CATALOG_PATH = os.path.join('outputs', 'final_enriched', 'cde_catalog_enriched.csv')
DIGIPATH_CDES_PATH = os.path.join('cdeCatalogs', 'digipathCDEs.csv')

# Output Path
OUTPUT_DIR = 'outputs/master'
FINAL_CATALOG_FILENAME = 'cde_catalog_master.csv'
os.makedirs(OUTPUT_DIR, exist_ok=True)
FINAL_CATALOG_PATH = os.path.join(OUTPUT_DIR, FINAL_CATALOG_FILENAME)

# --- HELPER FUNCTIONS ---
def is_missing(value):
    """Checks if a value is null, NaN, or an empty/whitespace string."""
    return pd.isna(value) or (isinstance(value, str) and not value.strip())

def normalize_and_combine(val1, val2, additional_tags=None):
    """Combines two pipe-separated strings and adds any extra tags."""
    set1 = set(str(val1).split('|')) if pd.notna(val1) and str(val1).strip() else set()
    set2 = set(str(val2).split('|')) if pd.notna(val2) and str(val2).strip() else set()
    
    combined = set1.union(set2)
    
    if additional_tags:
        combined.update(additional_tags)
        
    combined.discard('') # Remove any empty string elements
    return '|'.join(sorted(list(combined)))

# --- MAIN LOGIC ---
def merge_and_update():
    """
    Main function to merge, update, and deduplicate the CDE catalogs.
    """
    logging.info("--- Starting DigiPath CDE Merge and Update Process ---")

    # 1. Load Data Sources
    logging.info("Loading data sources...")
    try:
        main_df = pd.read_csv(MAIN_CATALOG_PATH, dtype=str, low_memory=False)
        main_df.set_index('ID', inplace=True, drop=False)
        logging.info(f"Loaded {len(main_df)} CDEs from the main catalog.")

        digipath_df = pd.read_csv(DIGIPATH_CDES_PATH, dtype=str, low_memory=False)
        logging.info(f"Loaded {len(digipath_df)} CDEs from the DigiPath catalog.")
        
        main_df['norm_title'] = main_df['title'].str.strip().str.lower()
        main_df['norm_var_name'] = main_df['variable_name'].str.strip().str.lower()

    except Exception as e:
        logging.error(f"Failed to load initial data. Error: {e}"); return

    # 2. Iterate, Match, Update, and Deduplicate
    logging.info("Processing DigiPath CDEs to update and deduplicate main catalog...")
    
    new_cdes_to_add = []
    ids_to_delete = set()
    updated_ids = set()

    for _, digipath_row in tqdm(digipath_df.iterrows(), total=len(digipath_df), desc="Merging and Updating"):
        new_title = str(digipath_row.get('title')).strip().lower() if pd.notna(digipath_row.get('title')) else ''
        new_var_name = str(digipath_row.get('variable_name')).strip().lower() if pd.notna(digipath_row.get('variable_name')) else ''

        # --- FIX: Ensure boolean masks have the same index as the main DataFrame ---
        false_series = pd.Series(False, index=main_df.index)
        title_match_mask = main_df['norm_title'] == new_title if new_title else false_series
        var_name_match_mask = main_df['norm_var_name'] == new_var_name if new_var_name else false_series
        
        combined_mask = title_match_mask | var_name_match_mask
        matching_rows = main_df[combined_mask]
        
        if not matching_rows.empty:
            primary_match_id = matching_rows.index[0]
            
            # Prevent updating a record that's already been designated for deletion
            if primary_match_id in ids_to_delete:
                continue

            for col in digipath_df.columns:
                if col in main_df.columns:
                    new_value = digipath_row[col]
                    if not is_missing(new_value):
                        if col == 'collections':
                            original_value = main_df.loc[primary_match_id, col]
                            main_df.loc[primary_match_id, col] = normalize_and_combine(original_value, new_value, ['DigiPath'])
                        else:
                            main_df.loc[primary_match_id, col] = new_value
            main_df.loc[primary_match_id, 'collections'] = normalize_and_combine(main_df.loc[primary_match_id, 'collections'], '', ['DigiPath'])

            updated_ids.add(primary_match_id)
            if len(matching_rows) > 1:
                ids_to_delete.update(matching_rows.index[1:])
        else:
            new_row_data = digipath_row.to_dict()
            new_row_data['collections'] = normalize_and_combine(new_row_data.get('collections'), '', ['DigiPath'])
            new_cdes_to_add.append(new_row_data)

    # 3. Perform final DataFrame manipulations
    logging.info(f"Update summary: {len(updated_ids)} CDEs updated, {len(ids_to_delete)} marked for deletion, {len(new_cdes_to_add)} new CDEs to be added.")
    
    main_df.drop(list(ids_to_delete), inplace=True, errors='ignore')
    
    if new_cdes_to_add:
        new_cdes_df = pd.DataFrame(new_cdes_to_add)
        main_df.reset_index(drop=True, inplace=True)
        numeric_ids = pd.to_numeric(main_df['ID'], errors='coerce').dropna()
        max_id = int(numeric_ids.max()) if not numeric_ids.empty else 0
        new_cdes_df['ID'] = range(max_id + 1, max_id + 1 + len(new_cdes_df))
        
        final_df = pd.concat([main_df, new_cdes_df], ignore_index=True)
    else:
        final_df = main_df.reset_index(drop=True)

    final_df.drop(columns=['norm_title', 'norm_var_name'], inplace=True, errors='ignore')
    
    # 4. Save the Final Master Catalog
    try:
        final_df.to_csv(FINAL_CATALOG_PATH, index=False, quoting=csv.QUOTE_ALL)
        logging.info(f"✅ Success! Master catalog saved to: {FINAL_CATALOG_PATH}")
        logging.info(f"The final master catalog contains {len(final_df)} CDEs.")
    except Exception as e:
        logging.error(f"Failed to save the master catalog. Error: {e}")

    logging.info("--- Merge and Update Process Complete ---")

if __name__ == "__main__":
    merge_and_update()
