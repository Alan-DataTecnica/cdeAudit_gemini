# enrich_catalog.py
# Purpose: A final, standalone script to apply automated enrichments to the
# polished CDE catalog, creating the definitive, shareable version.

import os
import pandas as pd
import json
import logging
import csv
import re
from tqdm import tqdm

# --- CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Input Paths
CATALOG_PATH = os.path.join('outputs', 'final_polished', 'cde_catalog_polished.csv')
SUGGESTIONS_DIR = os.path.join('outputs', 'stage_3', 'raw_responses')
ICD10_MAP_PATH = os.path.join('cdeCatalogs', 'icd10_cm.csv')

# Output Path
OUTPUT_DIR = 'outputs/final_enriched'
FINAL_FILENAME = 'cde_catalog_enriched.csv'
os.makedirs(OUTPUT_DIR, exist_ok=True)
FINAL_OUTPUT_PATH = os.path.join(OUTPUT_DIR, FINAL_FILENAME)

# --- DATA LOADING ---
def load_and_process_suggestions(suggestions_dir: str):
    """Aggregates all raw suggestions from Pass 1 into a structured format."""
    all_suggestions = {}
    if not os.path.exists(suggestions_dir):
        logging.warning(f"Suggestions directory not found. Cannot apply suggestions.")
        return {}
    files_to_process = [f for f in os.listdir(suggestions_dir) if f.endswith(('.json', '.txt'))]
    for filename in tqdm(files_to_process, desc="Loading AI Suggestions"):
        filepath = os.path.join(suggestions_dir, filename)
        with open(filepath, 'r', encoding='utf-8') as f:
            try:
                data, suggestions_list = json.loads(f.read()), []
                if isinstance(data, dict) and "candidates" in data:
                    text_content = data.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
                    if text_content: suggestions_list = json.loads(text_content)
                elif isinstance(data, list):
                    suggestions_list = data
                for sug in suggestions_list:
                    if cde_id := sug.get("ID"):
                        all_suggestions[str(cde_id)] = sug.get("suggestions", {})
            except Exception:
                continue
    return all_suggestions

# --- HELPER FUNCTIONS ---
def normalize_and_combine(val1, val2):
    """Combines two pipe-separated strings into a single, unique, sorted string."""
    set1 = set(str(val1).split('|')) if pd.notna(val1) else set()
    set2 = set(str(val2).split('|')) if pd.notna(val2) else set()
    combined = set1.union(set2)
    combined.discard('') # Remove any empty string elements
    return '|'.join(sorted(list(combined)))

def is_missing(value):
    """Checks if a value is null, NaN, or an empty/whitespace string."""
    return pd.isna(value) or (isinstance(value, str) and not value.strip())

def extract_icd_from_string(text: str):
    """Extracts an ICD10 code from a string, checking multiple patterns."""
    if not isinstance(text, str):
        return None
    match = re.search(r'\((?:ICD10,\s*([A-Z0-9\.]+))\)', text, re.IGNORECASE)
    if match: return match.group(1).strip()
    match = re.search(r'icd10_?([A-Z0-9\.]+)', text, re.IGNORECASE)
    if match: return match.group(1).strip()
    return None

# --- MAIN LOGIC ---
def enrich_catalog():
    """Main function to run the final enrichment process."""
    logging.info("--- Starting Final Catalog Enrichment ---")

    # 1. Load Data
    logging.info("Loading data sources...")
    try:
        df = pd.read_csv(CATALOG_PATH, dtype=str, low_memory=False)
        logging.info(f"Loaded {len(df)} CDEs from '{CATALOG_PATH}'.")
        icd10_df = pd.read_csv(ICD10_MAP_PATH, dtype=str)
        logging.info(f"Loaded {len(icd10_df)} records from '{ICD10_MAP_PATH}'.")
        
        # --- FIX: Normalize the lookup key to be case-insensitive ---
        icd10_df['lookup_key'] = icd10_df['variable_name'].str.replace('.', '', regex=False).str.upper()
        icd10_lookup = icd10_df.set_index('lookup_key').to_dict('index')
    except Exception as e:
        logging.error(f"Failed to load initial data. Error: {e}"); return

    all_suggestions = load_and_process_suggestions(SUGGESTIONS_DIR)

    # Task 1: Fill missing permissible_values
    logging.info("TASK 1: Applying 'permissible_values' suggestions...")
    pv_null_mask = df['permissible_values'].apply(is_missing)
    updated_pv_count = 0
    for index, row in df[pv_null_mask].iterrows():
        cde_id = row['ID']
        if cde_id in all_suggestions and 'permissible_values' in all_suggestions[cde_id]:
            df.loc[index, 'permissible_values'] = all_suggestions[cde_id]['permissible_values']
            updated_pv_count += 1
    logging.info(f"Populated 'permissible_values' for {updated_pv_count} of {pv_null_mask.sum()} CDEs.")

    # Task 2: Append synonymous_terms
    logging.info("TASK 2: Appending 'synonymous_terms' to 'alternate_titles'...")
    df['alternate_titles'] = df.apply(lambda row: normalize_and_combine(row['alternate_titles'], row['synonymous_terms']), axis=1)
    logging.info("Append operation complete.")
    
    # Task 3: Create 'found_icd_code' column
    logging.info("TASK 3: Extracting ICD codes to create 'found_icd_code' column...")
    search_cols = ['title', 'variable_name', 'source_header', 'collected_headers']
    for col in search_cols:
        if col not in df.columns: df[col] = ''
    df['found_icd_code'] = df[search_cols].apply(lambda row: next((code for col in search_cols if (code := extract_icd_from_string(row[col])) is not None), None), axis=1)
    logging.info(f"Found {df['found_icd_code'].notna().sum()} CDEs with an identifiable ICD code.")

    # Task 4: Enrich from ICD10 map
    logging.info("TASK 4: Enriching ICD-based CDEs from the official map...")
    icd_cde_mask = df['found_icd_code'].notna()
    
    match_in_map_count, updated_alt_titles_count, updated_alt_headers_count = 0, 0, 0
    
    has_alt_titles = 'alternate_titles' in icd10_df.columns
    has_alt_headers = 'alternate_headers' in icd10_df.columns

    for index, row in tqdm(df[icd_cde_mask].iterrows(), total=icd_cde_mask.sum(), desc="Enriching ICD CDEs"):
        icd_code_with_dot = row['found_icd_code']
        # --- FIX: Normalize the lookup key to be case-insensitive ---
        lookup_key = icd_code_with_dot.replace('.', '').upper()
        
        if lookup_key in icd10_lookup:
            match_in_map_count += 1
            icd_map_data = icd10_lookup[lookup_key]
            
            if has_alt_titles and not is_missing(map_alt_titles := icd_map_data.get('alternate_titles')):
                df.loc[index, 'alternate_titles'] = normalize_and_combine(row['alternate_titles'], map_alt_titles)
                updated_alt_titles_count += 1
            
            if has_alt_headers and not is_missing(map_alt_headers := icd_map_data.get('alternate_headers')):
                df.loc[index, 'alternate_headers'] = normalize_and_combine(row['alternate_headers'], map_alt_headers)
                updated_alt_headers_count += 1

    logging.info("--- Enrichment Sanity Check Report ---")
    logging.info(f"CDEs with an ICD code: {icd_cde_mask.sum()}")
    logging.info(f"Found a matching code in the ICD map for: {match_in_map_count} CDEs")
    logging.info(f"Matched records with non-empty 'alternate_titles' to append: {updated_alt_titles_count}")
    logging.info(f"Matched records with non-empty 'alternate_headers' to append: {updated_alt_headers_count}")

    # 5. Save the Final Enriched Catalog
    try:
        df.to_csv(FINAL_OUTPUT_PATH, index=False, quoting=csv.QUOTE_ALL)
        logging.info(f"✅ Success! Enriched catalog saved to: {FINAL_OUTPUT_PATH}")
    except Exception as e:
        logging.error(f"Failed to save the enriched catalog. Error: {e}")

    logging.info("--- Enrichment Process Complete ---")

if __name__ == "__main__":
    enrich_catalog()
