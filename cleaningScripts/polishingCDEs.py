# generate_polished_catalog.py
# Purpose: A standalone script to create a final, polished version of the CDE
# catalog by applying a specific set of programmatic rules and clean-up logic.

import os
import pandas as pd
import json
import logging
import csv
from tqdm import tqdm

# --- CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Input Paths
CATALOG_PATH = os.path.join('outputs', 'stage_4', 'cde_catalog_final.csv')
SUGGESTIONS_DIR = os.path.join('outputs', 'stage_3', 'raw_responses')

# Output Path
OUTPUT_DIR = 'outputs/final_polished'
FINAL_FILENAME = 'cde_catalog_polished.csv'
os.makedirs(OUTPUT_DIR, exist_ok=True)
FINAL_OUTPUT_PATH = os.path.join(OUTPUT_DIR, FINAL_FILENAME)

# --- DATA LOADING ---
# --- FIX: Removed @st.cache_data decorator as it's not a Streamlit app ---
def load_and_process_suggestions(suggestions_dir: str):
    """Aggregates all raw suggestions from Pass 1 into a structured format."""
    all_suggestions = {}
    if not os.path.exists(suggestions_dir):
        logging.error(f"Suggestions directory not found. Expected at: {suggestions_dir}")
        return {}
    files_to_process = [f for f in os.listdir(suggestions_dir) if f.endswith(('.json', '.txt'))]
    for filename in tqdm(files_to_process, desc="Loading AI Suggestions"):
        filepath = os.path.join(suggestions_dir, filename)
        with open(filepath, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
                suggestions_list = []
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
    """
    Normalizes a collection string (either pipe-separated or a string
    representation of a list) into a set of unique string values.
    """
    if not isinstance(collection_str, str) or not collection_str.strip():
        return set()
    # Handle case like "['ICD', 'Diagnosis']"
    if collection_str.startswith('[') and collection_str.endswith(']'):
        try:
            # Safely evaluate the string representation of a list
            collection_list = eval(collection_str)
            return {str(item).strip() for item in collection_list if str(item).strip()}
        except Exception:
            # Fallback for malformed list strings
            return set()
    # Handle pipe-separated case
    else:
        return {item.strip() for item in collection_str.split('|') if item.strip()}


# --- MAIN LOGIC ---
def polish_catalog():
    """
    Main function to run the polishing and filtering process.
    """
    logging.info("--- Starting Polished Catalog Generation ---")

    # 1. Load Data
    if not os.path.exists(CATALOG_PATH):
        logging.error(f"Final catalog not found at '{CATALOG_PATH}'. Aborting.")
        return
    df = pd.read_csv(CATALOG_PATH, dtype=str, low_memory=False)
    logging.info(f"Loaded {len(df)} CDEs from the final catalog.")
    
    all_suggestions = load_and_process_suggestions(SUGGESTIONS_DIR)
    if not all_suggestions:
        logging.warning("No AI suggestions loaded. Will only perform column formatting.")

    # 2. Identify target CDEs (where variable_name starts with 'icd10_')
    icd10_mask = df['variable_name'].str.startswith('icd10_', na=False)
    icd10_cde_ids = df[icd10_mask]['ID'].tolist()
    logging.info(f"Found {len(icd10_cde_ids)} CDEs with 'icd10_' variable names.")

    # 3. Create the new column, initialized with empty values
    df['standard_codes'] = pd.NA

    # 4. Process the identified CDEs
    logging.info("Applying changes to ICD10 CDEs...")
    for cde_id in tqdm(icd10_cde_ids, desc="Processing ICD10 CDEs"):
        # Get the row index for the current CDE
        idx = df[df['ID'] == cde_id].index[0]
        
        # a. Apply all Gemini suggestions for this CDE
        if cde_id in all_suggestions:
            suggestions = all_suggestions[cde_id]
            for field, value in suggestions.items():
                if field in df.columns:
                    df.loc[idx, field] = value
        
        # b. Populate the 'standard_codes' column
        variable_name = df.loc[idx, 'variable_name']
        if variable_name and isinstance(variable_name, str):
            code = variable_name.replace('icd10_', '').upper()
            df.loc[idx, 'standard_codes'] = str({'ICD10': code})

        # c. Update 'collections' to ensure 'ICD' is present
        original_collections = df.loc[idx, 'collections']
        collections_set = normalize_collections(original_collections)
        collections_set.add('ICD')
        df.loc[idx, 'collections'] = '|'.join(sorted(list(collections_set)))

    # 5. Normalize the entire 'collections' column for consistency
    logging.info("Normalizing the 'collections' column format for all CDEs...")
    df['collections'] = df['collections'].apply(lambda x: '|'.join(sorted(list(normalize_collections(x)))))

    # 6. Save the polished catalog
    try:
        df.to_csv(FINAL_OUTPUT_PATH, index=False, quoting=csv.QUOTE_ALL)
        logging.info(f"✅ Success! Polished catalog saved to: {FINAL_OUTPUT_PATH}")
        logging.info(f"The final polished catalog contains {len(df)} CDEs.")
    except Exception as e:
        logging.error(f"Failed to save the polished catalog. Error: {e}")

    print("--- Polishing Process Complete ---")

if __name__ == "__main__":
    polish_catalog()
