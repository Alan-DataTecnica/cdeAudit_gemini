import pandas as pd
import numpy as np
import sys
import os
import re
import json
import logging
import sqlite3
from collections import defaultdict

# --- Basic Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 1. CONFIGURATION ---
# --- NEW: Define paths for BOTH data sources ---
DATABASE_PATH = "cdeCatalogs/20250603_2030_cde.sqlite" # <-- Set path to your SQLite DB
TABLE_NAME = "CDE_Dictionary_Condensed"                    # <-- Set name of the table in your DB
ORIGINAL_CSV_PATH = 'cdeCatalogs/cdeCatalog.csv' # <-- Set path to the original CSV

MAPPING_FILE_PATH = 'mapping/permissible_values_map.csv'
OUTPUT_DIR = "outputs/stage_1"
FINAL_CATALOG_FILENAME = "cde_catalog_processed.csv"
PROVENANCE_LOG_FILENAME = "change_provenance_log.csv"
UNPARSABLE_LOG_FILENAME = "unparsable_values_log.csv"

# --- Column Names ---
COLUMN_MAP = {
    'ID': 'ID',
    'PV': 'permissible_values',
    'VF': 'value_format',
    'UM': 'unit_of_measure',
    'VM': 'value_mapping',
    'VAR_NAME': 'variable_name',
    'TITLE': 'title',
    'SHORT_DESC': 'short_description'
}
# --- End of Configuration ---

# The run_stage_1_processing function remains the same as before.
# All changes are in the main() function's data loading section.
def run_stage_1_processing(df: pd.DataFrame, mapping_dict: dict) -> tuple[pd.DataFrame, list, dict]:
    # ... (This function from your existing script does not need to be changed) ...
    # For brevity, its code is omitted here, but should be kept in your file.
    logging.info("Starting complete Stage 1 data processing workflow...")
    change_log = []
    summary_counters = defaultdict(int)
    
    # --- Ensure all required columns exist ---
    for col_key, col_name in COLUMN_MAP.items():
        if col_name not in df.columns:
            if col_key == 'ID':
                logging.error(f"Fatal: Required ID column '{col_name}' is missing.")
                sys.exit(1)
            df[col_name] = np.nan

    df_processed = df.copy()
    df_processed['pv_was_standardized'] = False
    
    # --- Get column names from map for easier access ---
    id_col, pv_col, vf_col, um_col, vm_col, var_name_col, title_col, desc_col = [COLUMN_MAP.get(k) for k in ['ID', 'PV', 'VF', 'UM', 'VM', 'VAR_NAME', 'TITLE', 'SHORT_DESC']]

    # --- Step 1: Pre-Cleaning of PV Column ---
    logging.info("Step 1: Applying pre-cleaning rules...")
    df_processed[pv_col] = df_processed[pv_col].astype(str).fillna('').str.strip()
    for phrase in ['Permissible values range', 'Permissible values']:
        df_processed[pv_col] = df_processed[pv_col].str.replace(phrase, '', case=False, regex=False)
    df_processed[pv_col] = df_processed[pv_col].str.strip().str.lstrip(':')
    df_processed.loc[df_processed[pv_col].isin(['1', 'Response']), pv_col] = ''
    
    # --- Step 2: Permissible Values Standardization (Two-Pass) ---
    logging.info("Step 2: Standardizing 'permissible_values' column...")
    df_processed['__processed_pv'] = False
    for index, row in df_processed.iterrows():
        original_value = str(row[pv_col]).strip()
        if not original_value or original_value.lower() == 'nan':
            df_processed.loc[index, '__processed_pv'] = True
            continue
        
        if original_value in mapping_dict:
            cde_id = row[id_col]
            map_entry = mapping_dict[original_value]
            summary_counters['transformed_from_map'] += 1
            for key, std_val in map_entry.items():
                target_col = COLUMN_MAP.get(key)
                if pd.notna(std_val) and std_val != '' and target_col:
                    original_target_val = row[target_col]
                    df_processed.loc[index, target_col] = std_val
                    change_log.append({
                        'cde_id': cde_id,
                        'column_changed': target_col,
                        'action_taken': f'Applied from map: {original_value}',
                        'original_value': original_target_val,
                        'new_value': std_val
                    })
            df_processed.loc[index, 'pv_was_standardized'] = True
            df_processed.loc[index, '__processed_pv'] = True
    df_processed.drop(columns=['__processed_pv'], inplace=True)
    
    # --- Step 3: General Quality Heuristics for All Fields ---
    logging.info("Step 3: Applying general quality heuristics to all key fields...")
    is_null_var = pd.isna(df_processed[var_name_col]) | (df_processed[var_name_col] == '')
    is_bad_format = ~df_processed[var_name_col].astype(str).str.match(r'^[a-z_][a-z0-9_]*$', na=False)
    is_too_long = df_processed[var_name_col].astype(str).str.len() > 30
    df_processed['flag_bad_variable_name'] = is_null_var | is_bad_format | is_too_long
    summary_counters['flagged_bad_variable_name'] = int(df_processed['flag_bad_variable_name'].sum())

    is_null_title = pd.isna(df_processed[title_col]) | (df_processed[title_col] == '')
    is_too_short = df_processed[title_col].astype(str).str.split().str.len() < 3
    df_processed['flag_bad_title'] = is_null_title | is_too_short
    summary_counters['flagged_bad_title'] = int(df_processed['flag_bad_title'].sum())

    is_null_desc = pd.isna(df_processed[desc_col]) | (df_processed[desc_col] == '')
    is_desc_too_short = df_processed[desc_col].astype(str).str.split().str.len() < 5
    is_redundant = (df_processed[title_col] == df_processed[desc_col]) & (df_processed[title_col] != '')
    df_processed['flag_bad_description'] = is_null_desc | is_desc_too_short | is_redundant
    summary_counters['flagged_bad_description'] = int(df_processed['flag_bad_description'].sum())
    
    # --- Step 4: Final PV Quality Check ---
    logging.info("Step 4: Running final check on 'permissible_values' structure...")
    is_free_entry = df_processed[vf_col].str.lower() == 'free entry'
    is_constraint = df_processed[pv_col].astype(str).str.match(r'^\(y\s*[<>=!].*\)$', na=False)
    is_pipe = df_processed[pv_col].astype(str).str.contains('|', regex=False, na=False)
    is_empty_or_nan = pd.isna(df_processed[pv_col]) | (df_processed[pv_col].astype(str).isin(['', 'nan']))
    is_structured = is_pipe | is_constraint | is_empty_or_nan
    df_processed['flag_bad_permissibles'] = ~is_structured & ~is_free_entry
    summary_counters['flagged_bad_permissibles'] = int(df_processed['flag_bad_permissibles'].sum())

    # --- Step 5: Create Final Audit Flag ---
    flag_cols = [col for col in df_processed.columns if col.startswith('flag_')]
    df_processed['needs_audit'] = df_processed[flag_cols].any(axis=1)
    summary_counters['total_cde_needs_audit'] = int(df_processed['needs_audit'].sum())
    
    return df_processed, change_log, summary_counters

def main():
    """Main function to run the complete, full-scope Stage 1 process."""
    # Setup Paths
    output_path = os.path.join(OUTPUT_DIR, FINAL_CATALOG_FILENAME)
    provenance_path = os.path.join(OUTPUT_DIR, PROVENANCE_LOG_FILENAME)
    unparsable_path = os.path.join(OUTPUT_DIR, UNPARSABLE_LOG_FILENAME)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # --- START: New Data Integration Logic ---
    try:
        # 1. Load data from the SQLite database
        logging.info(f"Connecting to database: {DATABASE_PATH}")
        conn = sqlite3.connect(DATABASE_PATH)
        query = f"SELECT * FROM {TABLE_NAME}"
        df_sqlite = pd.read_sql_query(query, conn)
        conn.close()
        logging.info(f"Successfully loaded {len(df_sqlite)} rows from the SQLite database.")
        
        # Clean SQLite data: ensure ID is a string and not null
        df_sqlite.dropna(subset=['ID'], inplace=True)
        df_sqlite['ID'] = df_sqlite['ID'].astype(str)

        # 2. Load data from the original CSV file
        logging.info(f"Loading original CDE catalog from: {ORIGINAL_CSV_PATH}")
        df_csv = pd.read_csv(ORIGINAL_CSV_PATH, sep=',', engine='python', on_bad_lines='warn', dtype={'ID': str})
        
        # Clean CSV data: drop rows without an ID
        df_csv.dropna(subset=['ID'], inplace=True)
        logging.info(f"Successfully loaded {len(df_csv)} rows with valid IDs from the CSV file.")

        # 3. Merge the two data sources
        logging.info("Merging data from SQLite and CSV sources...")
        # Use an outer merge to keep all records from both sources
        # Use suffixes to distinguish columns that exist in both
        df_merged = pd.merge(df_csv, df_sqlite, on='ID', how='outer', suffixes=('_csv', '_sqlite'))

        # 4. Coalesce columns, prioritizing the CSV file for competing values
        common_cols = [col.replace('_csv', '') for col in df_merged.columns if '_csv' in col]
        
        for col in common_cols:
            csv_col = f"{col}_csv"
            sqlite_col = f"{col}_sqlite"
            # The CSV value takes priority. If it's missing, the SQLite value is used.
            df_merged[col] = df_merged[csv_col].combine_first(df_merged[sqlite_col])
        
        # Drop the temporary, suffixed columns
        cols_to_drop = [col for col in df_merged.columns if '_csv' in col or '_sqlite' in col]
        df_merged.drop(columns=cols_to_drop, inplace=True)
        
        df_cde = df_merged # This is now our master DataFrame for processing
        logging.info(f"Merge complete. Resulting catalog has {len(df_cde)} CDEs.")
        
        # Load the mapping file
        logging.info(f"Loading mapping file from: {MAPPING_FILE_PATH}")
        df_map = pd.read_csv(MAPPING_FILE_PATH, keep_default_na=False)
        mapping_dict = {
            row['original_expression'].strip(): {
                'PV': row['standardized_pv'], 'UM': row['standardized_unit'],
                'VF': row['standardized_value_format'], 'VM': row['standardized_value_mapping']
            } for _, row in df_map.iterrows()
        }
    except Exception as e:
        logging.error(f"A critical error occurred during data loading and merging: {e}")
        sys.exit(1)
    # --- END: New Data Integration Logic ---

    # --- Run Full Processing Workflow on the unified data ---
    df_processed, change_log, summary_counters = run_stage_1_processing(df_cde, mapping_dict)
    
    # ... (The rest of the main function for saving outputs and printing the summary remains the same) ...
    unparsable_df = df_processed[df_processed['flag_bad_permissibles']].copy()
    if not unparsable_df.empty:
        logging.warning(f"Found {len(unparsable_df)} CDEs with unparsable 'permissible_values' metadata. Saving to dump file.")
        unparsable_df[[COLUMN_MAP['ID'], COLUMN_MAP['PV']]].to_csv(unparsable_path, index=False)
    
    # Final cleaning step before saving
    final_cols_to_drop = [col for col in df_processed.columns if isinstance(col, str) and 'Unnamed:' in col]
    if final_cols_to_drop:
        df_processed.drop(columns=final_cols_to_drop, inplace=True, errors='ignore')
        logging.info(f"Removed final unwanted columns: {final_cols_to_drop}")

    logging.info(f"Saving processed catalog to: {output_path}")
    df_processed.to_csv(output_path, index=False)
    
    if change_log:
        logging.info(f"Saving provenance log with {len(change_log)} entries to: {provenance_path}")
        pd.DataFrame(change_log).to_csv(provenance_path, index=False)

    logging.info("--- Stage 1 Summary Report ---")
    if not summary_counters:
        logging.info("No transformations or flags were generated.")
    else:
        for action, count in sorted(summary_counters.items()):
            logging.info(f"{action.replace('_', ' ').title():<35}: {count}")
    logging.info("------------------------------")
    
    logging.info("Stage 1 complete.")


if __name__ == "__main__":
    main()