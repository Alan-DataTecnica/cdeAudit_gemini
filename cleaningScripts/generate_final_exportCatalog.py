# generate_final_product.py
# Purpose: The definitive final script to produce the polished, shareable CDE
# catalog in both CSV and SQLite formats. It performs final quality filters
# and applies all automated enrichments.

import os
import pandas as pd
import logging
import csv
import sqlite3
import json
from tqdm import tqdm

# --- CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Input Paths
MASTER_CATALOG_PATH = os.path.join('outputs', 'master', 'cde_catalog_master.csv')
SUGGESTIONS_DIR = os.path.join('outputs', 'stage_3', 'raw_responses')

# Output Paths
OUTPUT_DIR = 'outputs/sharable_product'
FINAL_CSV_FILENAME = 'cde_catalog_sharable.csv'
FINAL_SQLITE_FILENAME = 'cde_catalog_sharable.sqlite'
os.makedirs(OUTPUT_DIR, exist_ok=True)
FINAL_CSV_PATH = os.path.join(OUTPUT_DIR, FINAL_CSV_FILENAME)
FINAL_SQLITE_PATH = os.path.join(OUTPUT_DIR, FINAL_SQLITE_FILENAME)

# The specific columns for the final output
FINAL_COLUMNS = [
    'ID', 'variable_name', 'title', 'short_description', 
    'preferred_question_text', 'permissible_values', 'unit_of_measure', 
    'value_format', 'alternate_titles', 'alternate_headers', 'collections',
    'flag_missing_collection'
]

# --- HELPER FUNCTIONS ---
def is_missing(value):
    """Checks if a value is null, NaN, or an empty/whitespace string."""
    return pd.isna(value) or (isinstance(value, str) and not value.strip())

def normalize_and_combine(val1, val2):
    """Combines two values (which can be strings or lists) into a single, unique, sorted, pipe-separated string."""
    final_set = set()
    for val in [val1, val2]:
        if is_missing(val):
            continue
        # Handle string representation of a list, e.g., "['Clinical']"
        if isinstance(val, str) and val.startswith('[') and val.endswith(']'):
            try:
                # Safely evaluate the list string
                list_val = eval(val)
                if isinstance(list_val, list):
                    final_set.update(str(item).strip() for item in list_val if not is_missing(item))
            except Exception:
                 # Fallback for malformed list string, treat as a single item
                final_set.add(val.strip())
        # Handle pipe-separated string
        else:
            final_set.update(item.strip() for item in str(val).split('|') if not is_missing(item))
            
    return '|'.join(sorted(list(final_set)))


# --- DATA LOADING ---
def load_and_process_suggestions(suggestions_dir: str):
    """Aggregates all raw suggestions from Pass 1 into a structured format."""
    all_suggestions = {}
    if not os.path.exists(suggestions_dir):
        logging.warning(f"Suggestions directory not found at {suggestions_dir}. Cannot apply collection suggestions.")
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


# --- MAIN LOGIC ---
def generate_final_product():
    """
    Main function to run the final filtering and export process.
    """
    logging.info("--- Starting Generation of Final Sharable Product ---")

    # 1. Load the Master Catalog
    logging.info(f"Loading master catalog from: {MASTER_CATALOG_PATH}")
    if not os.path.exists(MASTER_CATALOG_PATH):
        logging.error(f"Master catalog not found at '{MASTER_CATALOG_PATH}'. Aborting.")
        return
    try:
        df = pd.read_csv(MASTER_CATALOG_PATH, dtype=str, low_memory=False)
        logging.info(f"Successfully loaded {len(df)} CDEs from the master catalog.")
    except Exception as e:
        logging.error(f"Could not read the master catalog file. Error: {e}")
        return

    # --- NEW: Load AI suggestions ---
    all_suggestions = load_and_process_suggestions(SUGGESTIONS_DIR)

    # 2. Perform Final Quality Filtering based on your new rules
    logging.info("Applying final quality filters to remove partial CDEs...")
    initial_count = len(df)
    
    # Rule 1: Drop if core identifiers are missing
    rule1_mask = df['ID'].apply(is_missing) | df['variable_name'].apply(is_missing) | df['title'].apply(is_missing)
    df = df[~rule1_mask]
    logging.info(f"Removed {rule1_mask.sum()} CDEs missing a core identifier.")

    # Rule 2: Drop if core content is missing
    rule2_mask = df['short_description'].apply(is_missing) & df['permissible_values'].apply(is_missing)
    df = df[~rule2_mask]
    logging.info(f"Removed {rule2_mask.sum()} CDEs missing BOTH description AND permissible values.")
    
    df_filtered = df.copy()
    logging.info(f"Total CDEs removed by quality filters: {initial_count - len(df_filtered)}. Remaining CDEs: {len(df_filtered)}")

    # --- NEW: Auto-accept and append collection suggestions ---
    logging.info("Appending AI-suggested collections to existing collections...")
    updated_collections_count = 0
    for index, row in tqdm(df_filtered.iterrows(), total=len(df_filtered), desc="Updating Collections"):
        cde_id = row['ID']
        original_collections = row['collections']
        
        # Check if there is an AI suggestion for this CDE's collections
        if cde_id in all_suggestions and (suggested_collections := all_suggestions[cde_id].get('collections')):
            # Combine original and suggested, then update the dataframe
            combined_collections = normalize_and_combine(original_collections, suggested_collections)
            df_filtered.loc[index, 'collections'] = combined_collections
            updated_collections_count += 1
    logging.info(f"Enriched the 'collections' field for {updated_collections_count} CDEs.")

    # 3. Flag CDEs that lack a collections tag
    logging.info("Flagging CDEs with missing 'collections' tag...")
    df_filtered['flag_missing_collection'] = df_filtered['collections'].apply(is_missing)
    logging.info(f"Flagged {df_filtered['flag_missing_collection'].sum()} CDEs with a missing 'collections' tag.")

    # 4. Select and Reorder Final Columns
    logging.info("Selecting and reordering columns for the final output...")
    for col in FINAL_COLUMNS:
        if col not in df_filtered.columns:
            df_filtered[col] = ''
    final_df = df_filtered[FINAL_COLUMNS]

    # 5. Save Final Products
    final_count = len(final_df)
    logging.info(f"The final polished catalog contains {final_count:,} CDEs.")

    # Save to CSV
    try:
        final_df.to_csv(FINAL_CSV_PATH, index=False, quoting=csv.QUOTE_ALL)
        logging.info(f"✅ Success! Sharable CSV saved to: {FINAL_CSV_PATH}")
    except Exception as e:
        logging.error(f"Failed to save the final CSV. Error: {e}")

    # Save to SQLite
    try:
        conn = sqlite3.connect(FINAL_SQLITE_PATH)
        final_df.to_sql('cde_catalog', conn, if_exists='replace', index=False)
        conn.close()
        logging.info(f"✅ Success! Sharable SQLite database saved to: {FINAL_SQLITE_PATH}")
    except Exception as e:
        logging.error(f"Failed to save the final SQLite database. Error: {e}")

    logging.info("--- Final Product Generation Complete ---")


if __name__ == "__main__":
    generate_final_product()
