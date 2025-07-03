# generate_sharable_catalog.py
# Purpose: The definitive final script to produce the polished, shareable CDE
# catalog in both CSV and SQLite formats. It applies final transformations
# and selects only the specified columns for the final output.

import os
import pandas as pd
import logging
import csv
import sqlite3
import json
import re
from tqdm import tqdm

# --- CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Input Paths
MASTER_CATALOG_PATH = os.path.join('outputs', 'sharable_product', '20250627_cdeCatalog.csv')
SUGGESTIONS_DIR = os.path.join('outputs', 'stage_3', 'raw_responses')

# Output Paths
OUTPUT_DIR = 'outputs/sharable_product'
FINAL_CSV_FILENAME = 'cde_catalog_sharable.csv'
FINAL_SQLITE_FILENAME = 'cde_catalog_sharable.sqlite'
os.makedirs(OUTPUT_DIR, exist_ok=True)
FINAL_CSV_PATH = os.path.join(OUTPUT_DIR, FINAL_CSV_FILENAME)
FINAL_SQLITE_PATH = os.path.join(OUTPUT_DIR, FINAL_SQLITE_FILENAME)

# The specific 11 columns for the final sharable output
FINAL_COLUMNS = [
    'ID', 'variable_name', 'title', 'short_description', 
    'preferred_question_text', 'permissible_values', 'unit_of_measure', 
    'value_format', 'synonymous_terms', 'collected_headers', 'collections'
]

# Abbreviation and Stop Word Lists
ABBREVIATION_MAP = {
    'number': 'num', 'diagnosis': 'dx', 'treatment': 'trt', 'assessment': 'asmt', 'history': 'hx', 
    'medical': 'med', 'clinical': 'clin', 'symptom': 'sympt', 'procedure': 'proc', 'examination': 'exam', 
    'age': 'ag', 'date': 'dt', 'year': 'yr', 'month': 'mo', 'day': 'dy', 'daily': 'dly', 'weekly': 'wkly', 
    'duration': 'dur', 'period': 'prd', 'onset': 'ons', 'left': 'lft', 'right': 'rt', 'unspecified': 'unsp', 
    'with': 'w', 'without': 'wo', 'amount': 'amt', 'average': 'avg', 'total': 'tot', 'count': 'cnt', 
    'measure': 'msr', 'index': 'idx', 'score': 'scr', 'level': 'lvl', 'value': 'val', 'percent': 'pct', 
    'percentage': 'pct', 'rate': 'rt', 'frequency': 'freq', 'volume': 'vol', 'size': 'sz', 'weight': 'wt', 
    'question': 'qst', 'response': 'resp', 'summary': 'summ', 'description': 'desc', 'identifier': 'id', 
    'category': 'cat', 'type': 'typ', 'status': 'stat', 'change': 'chg', 'difference': 'diff'
}
STOP_WORDS = {'a', 'an', 'the', 'of', 'in', 'and', 'for', 'to', 'is', 'are'}


# --- HELPER FUNCTIONS ---
def load_and_process_suggestions(suggestions_dir: str):
    """Aggregates all raw AI suggestions into a dictionary."""
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

def generate_variable_name(title: str, alt_titles: str) -> str:
    """Generates a standardized variable_name from titles."""
    titles = [title]
    if pd.notna(alt_titles):
        titles.extend(alt_titles.split('|'))
    titles = [t for t in titles if pd.notna(t) and t.strip()]
    if not titles: return ''
    shortest_title = min(titles, key=len)
    cleaned_title = re.sub(r'\(.*?\)', '', shortest_title).strip()
    cleaned_title = re.sub(r'[^A-Za-z0-9 ]+', '', cleaned_title).lower()
    words = cleaned_title.split()
    processed_words = [ABBREVIATION_MAP.get(word, word) for word in words if word not in STOP_WORDS]
    snake_case_name = '_'.join(processed_words)
    return snake_case_name[:25]
    
def normalize_and_combine(val1, val2):
    """Combines two values into a single, unique, sorted, pipe-separated string."""
    final_set = set()
    for val in [val1, val2]:
        if pd.isna(val) or not str(val).strip(): continue
        if isinstance(val, str) and val.startswith('[') and val.endswith(']'):
            try:
                final_set.update(str(item).strip() for item in eval(val) if str(item).strip())
            except Exception:
                final_set.add(val.strip())
        else:
            final_set.update(item.strip() for item in str(val).split('|') if item.strip())
    return '|'.join(sorted(list(final_set)))

# --- MAIN LOGIC ---
def generate_final_product():
    """Main function to run the final filtering and export process."""
    logging.info("--- Starting Generation of Final Sharable Product ---")

    # 1. Load the Master Catalog
    logging.info(f"Attempting to load master catalog from: {os.path.abspath(MASTER_CATALOG_PATH)}")
    if not os.path.exists(MASTER_CATALOG_PATH):
        logging.error(f"Master catalog not found. Aborting.")
        return
    try:
        df = pd.read_csv(MASTER_CATALOG_PATH, dtype=str, low_memory=False)
        logging.info(f"✅ Input file loaded successfully. Shape: {df.shape[0]} rows x {df.shape[1]} columns.")
        logging.info(f"Input columns: {list(df.columns)}")
    except Exception as e:
        logging.error(f"Could not read the master catalog file. Error: {e}")
        return

    # 2. Load AI Suggestions
    all_suggestions = load_and_process_suggestions(SUGGESTIONS_DIR)

    # 3. Apply Final Transformations
    logging.info("Applying final transformations to the catalog...")

    # a. Append AI-suggested collections
    updated_collections_count = 0
    if all_suggestions:
        for index, row in tqdm(df.iterrows(), total=len(df), desc="Appending AI Collections"):
            cde_id = row['ID']
            if cde_id in all_suggestions and (suggested_collections := all_suggestions[cde_id].get('collections')):
                df.loc[index, 'collections'] = normalize_and_combine(row['collections'], suggested_collections)
                updated_collections_count += 1
    logging.info(f"Enriched the 'collections' field for {updated_collections_count} CDEs based on AI suggestions.")

    # b. Standardize remaining ICD variable names
    update_mask = df['variable_name'].str.startswith('icd10_', na=False)
    num_to_update = update_mask.sum()
    if num_to_update > 0:
        logging.info(f"Found {num_to_update} 'icd10_' variable names to standardize...")
        for index, row in tqdm(df[update_mask].iterrows(), total=num_to_update, desc="Standardizing Variable Names"):
            new_name = generate_variable_name(row['title'], row['synonymous_terms'])
            if new_name:
                df.loc[index, 'variable_name'] = new_name
    logging.info(f"Standardized {num_to_update} variable names.")

    # 4. Select and Reorder Final Columns
    logging.info("Filtering DataFrame to include only the 11 specified final columns...")
    
    # Create a new DataFrame with only the desired columns
    # This prevents any other columns from being carried over
    final_df = pd.DataFrame()
    for col in FINAL_COLUMNS:
        if col in df.columns:
            final_df[col] = df[col]
        else:
            logging.warning(f"Column '{col}' not found in the source data. It will be added as an empty column.")
            final_df[col] = ''
            
    logging.info(f"✅ Final DataFrame created. Shape: {final_df.shape[0]} rows x {final_df.shape[1]} columns.")
    logging.info(f"Final columns: {list(final_df.columns)}")

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
