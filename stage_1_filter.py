import pandas as pd
import numpy as np
import sys
import os
import re
import json
import logging
from collections import defaultdict

# --- Basic Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 1. CONFIGURATION ---
# Update this path to point to your new TSV file
CDE_CATALOG_PATH = 'cdeCatalogs/cdeCatalog.tsv'
MAPPING_FILE_PATH = 'mapping/permissible_values_map.csv'

OUTPUT_DIR = "outputs/stage_1"
FINAL_CATALOG_FILENAME = "cde_catalog_processed.csv"
PROVENANCE_LOG_FILENAME = "change_provenance_log.csv"
UNPARSABLE_LOG_FILENAME = "unparsable_values_log.csv"

# -- Quality Control --
# CDEs with this many (or more) quality flags from Stage 1 will be completely removed.
REJECTION_THRESHOLD = 3

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



def run_stage_1_processing(df: pd.DataFrame, mapping_dict: dict) -> tuple[pd.DataFrame, list, dict]:
    
    """    Applies the complete, multi-pass Stage 1 processing and quality checks in a single function."""
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
    # Rule A: Remove specific phrases
    for phrase in ['Permissible values range', 'Permissible values']:
        df_processed[pv_col] = df_processed[pv_col].str.replace(phrase, '', case=False, regex=False)
    # Rule B: Remove leading ':'
    df_processed[pv_col] = df_processed[pv_col].str.strip().str.lstrip(':')
    # Rule C & D: Remove rows where the value is exactly '1' or 'Response'
    df_processed.loc[df_processed[pv_col].isin(['1', 'Response']), pv_col] = ''
    
    # --- Step 2: Permissible Values Standardization (Two-Pass) ---
    logging.info("Step 2: Standardizing 'permissible_values' column...")
    df_processed['__processed_pv'] = False

    # Pass 2a: Direct Mapping from Dictionary
    for index, row in df_processed.iterrows():
        original_value = str(row[pv_col]).strip()
        if not original_value or original_value.lower() == 'nan':
            df_processed.loc[index, '__processed_pv'] = True
            continue
        
        if original_value in mapping_dict:
            cde_id = row[id_col]
            map_entry = mapping_dict[original_value]
            summary_counters['transformed_from_map'] += 1
            
            # Apply all defined transformations from the map
            for key, std_val in map_entry.items():
                target_col = COLUMN_MAP.get(key)
                # Check if there is a value to apply and a valid target column
                if pd.notna(std_val) and std_val != '' and target_col:
                    # Always overwrite the target column with the standardized value
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
    
    # Pass 2b: General Regex for Remainder (if any)
    # This can be expanded in the future if new general patterns are found
    
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
    input_path = CDE_CATALOG_PATH
    output_path = os.path.join(OUTPUT_DIR, FINAL_CATALOG_FILENAME)
    provenance_path = os.path.join(OUTPUT_DIR, PROVENANCE_LOG_FILENAME)
    unparsable_path = os.path.join(OUTPUT_DIR, UNPARSABLE_LOG_FILENAME)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Load Data
    try:
        logging.info(f"Loading CDE catalog from: {input_path}")
        df_cde = pd.read_csv(input_path, sep='\t', engine='python', on_bad_lines='warn')
        
        logging.info(f"Loading mapping file from: {MAPPING_FILE_PATH}")
        df_map = pd.read_csv(MAPPING_FILE_PATH, keep_default_na=False)
        mapping_dict = {
            row['original_expression'].strip(): {
                'PV': row['standardized_pv'], 'UM': row['standardized_unit'],
                'VF': row['standardized_value_format'], 'VM': row['standardized_value_mapping']
            } for _, row in df_map.iterrows()
        }
    except FileNotFoundError as e:
        logging.error(f"Fatal: Input file not found. {e}")
        sys.exit(1)
    
    # --- Run Full Processing Workflow ---
    df_processed, change_log, summary_counters = run_stage_1_processing(df_cde, mapping_dict)
    
    # --- Save Outputs ---
    unparsable_df = df_processed[df_processed['flag_bad_permissibles']].copy()
    if not unparsable_df.empty:
        logging.warning(f"Found {len(unparsable_df)} CDEs with unparsable 'permissible_values' metadata. Saving to dump file.")
        unparsable_df[[COLUMN_MAP['ID'], COLUMN_MAP['PV']]].to_csv(unparsable_path, index=False)
    
    # --- START: New Cleaning Step ---
    # This is the new block of code to add.
    # It cleans the DataFrame before it is saved.
    logging.info("Cleaning final DataFrame: Removing empty and 'Unnamed:' columns...")
    
    # Identify all columns that start with 'Unnamed:'
    cols_to_drop = [col for col in df_processed.columns if isinstance(col, str) and 'Unnamed:' in col]

    if cols_to_drop:
        df_processed.drop(columns=cols_to_drop, inplace=True, errors='ignore')
        logging.info(f"Removed the following unwanted columns: {cols_to_drop}")
    else:
        logging.info("No 'Unnamed:' columns found to remove.")
    # --- END: New Cleaning Step ---

    logging.info(f"Saving cleaned and processed catalog to: {output_path}")
    df_processed.to_csv(output_path, index=False)
    
    if change_log:
        logging.info(f"Saving provenance log with {len(change_log)} entries to: {provenance_path}")
        pd.DataFrame(change_log).to_csv(provenance_path, index=False)

    # --- Print Summary ---
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