# finalize_catalog.py
# Purpose: The definitive final script to produce the polished CDE catalog.
# It takes the manually reviewed catalog, merges data from duplicates, applies
# automated deduplication, and applies final programmatic improvements.

import os
import pandas as pd
import json
import logging
import csv
from tqdm import tqdm

# --- CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Input Paths
CATALOG_TO_FINALIZE_PATH = os.path.join('outputs', 'stage_4', 'cde_catalog_final.csv')
DEDUPLICATION_REPORT_PATH = os.path.join('outputs', 'final_polished', 'deduplication_report.csv')
SUGGESTIONS_DIR = os.path.join('outputs', 'stage_3', 'raw_responses')

# Output Path
OUTPUT_DIR = 'outputs/final_polished'
FINAL_POLISHED_FILENAME = 'cde_catalog_polished.csv'
FINAL_OUTPUT_PATH = os.path.join(OUTPUT_DIR, FINAL_POLISHED_FILENAME)


# --- DATA LOADING ---
def load_and_process_suggestions(suggestions_dir: str):
    """Aggregates all raw suggestions from Pass 1 into a structured format."""
    all_suggestions = {}
    if not os.path.exists(suggestions_dir):
        logging.warning(f"Suggestions directory not found at: {suggestions_dir}. Cannot apply AI suggestions.")
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
def normalize_collections(collection_str: str) -> set:
    """Normalizes a collection string into a set of unique values."""
    if not isinstance(collection_str, str) or not collection_str.strip():
        return set()
    if collection_str.startswith('[') and collection_str.endswith(']'):
        try:
            return {str(item).strip() for item in eval(collection_str) if str(item).strip()}
        except Exception:
            return set()
    else:
        return {item.strip() for item in collection_str.split('|') if item.strip()}


# --- MAIN LOGIC ---
def finalize():
    """Main function to run the finalization process."""
    logging.info("--- Starting Final Catalog Polishing and Finalization ---")

    # 1. Load Data
    logging.info("Loading data sources...")
    if not os.path.exists(CATALOG_TO_FINALIZE_PATH):
        logging.error(f"Catalog to finalize not found at '{CATALOG_TO_FINALIZE_PATH}'. Aborting.")
        return
    df = pd.read_csv(CATALOG_TO_FINALIZE_PATH, dtype=str, low_memory=False)
    df.set_index('ID', inplace=True, drop=False) # Use ID as index for easy lookups
    
    all_suggestions = load_and_process_suggestions(SUGGESTIONS_DIR)

    # 2. Perform Intelligent Deduplication
    ids_to_delete = set()
    if os.path.exists(DEDUPLICATION_REPORT_PATH):
        logging.info("Loading deduplication report to merge data and remove duplicates...")
        report_df = pd.read_csv(DEDUPLICATION_REPORT_PATH, dtype=str)
        
        for _, row in tqdm(report_df.iterrows(), total=len(report_df), desc="Merging duplicate data"):
            original_id = row['Original_CDE_ID']
            duplicate_id = row['Duplicate_CDE_ID']
            
            if original_id not in df.index or duplicate_id not in df.index:
                continue # Skip if either CDE doesn't exist anymore

            # --- NEW: Merge data from duplicate to original before deletion ---
            for col in df.columns:
                original_val = df.loc[original_id, col]
                duplicate_val = df.loc[duplicate_id, col]
                # If original is empty but duplicate has data, copy it over.
                if pd.isna(original_val) or str(original_val).strip() == '':
                    if pd.notna(duplicate_val) and str(duplicate_val).strip() != '':
                        df.loc[original_id, col] = duplicate_val

            # Add the duplicate ID to the set for deletion
            ids_to_delete.add(duplicate_id)
        
        initial_count = len(df)
        df.drop(list(ids_to_delete), inplace=True)
        logging.info(f"Merged and removed {len(ids_to_delete)} duplicate CDEs based on the report.")
    else:
        logging.warning("Deduplication report not found. Skipping automated deduplication.")

    # 3. Apply Final Programmatic Improvements to ICD10 CDEs
    logging.info("Applying final programmatic improvements to ICD10 CDEs...")
    icd10_mask = df['variable_name'].str.startswith('icd10_', na=False)
    icd10_cde_ids = df[icd10_mask]['ID'].tolist()

    for cde_id in tqdm(icd10_cde_ids, desc="Finalizing ICD10 CDEs"):
        if cde_id in all_suggestions:
            suggestions = all_suggestions[cde_id]
            for field, value in suggestions.items():
                if field == 'collections':
                    original_collections = normalize_collections(df.loc[cde_id, 'collections'])
                    suggested_collections = normalize_collections(str(value))
                    combined = original_collections.union(suggested_collections)
                    df.loc[cde_id, 'collections'] = '|'.join(sorted(list(combined)))
                elif field in df.columns:
                    df.loc[cde_id, field] = value
        
        collections_set = normalize_collections(df.loc[cde_id, 'collections'])
        collections_set.add('ICD')
        df.loc[cde_id, 'collections'] = '|'.join(sorted(list(collections_set)))

    # 4. Final Normalization of 'collections' column
    logging.info("Performing final normalization of the 'collections' column format...")
    df['collections'] = df['collections'].apply(lambda x: '|'.join(sorted(list(normalize_collections(x)))))
    
    # 5. Save the final polished catalog
    df.reset_index(drop=True, inplace=True) # Reset index before saving
    try:
        df.to_csv(FINAL_OUTPUT_PATH, index=False, quoting=csv.QUOTE_ALL)
        logging.info(f"✅ Success! Polished catalog saved to: {FINAL_OUTPUT_PATH}")
        logging.info(f"The final polished catalog contains {len(df)} CDEs.")
    except Exception as e:
        logging.error(f"Failed to save the polished catalog. Error: {e}")

    logging.info("--- Finalization Process Complete ---")

if __name__ == "__main__":
    finalize()
