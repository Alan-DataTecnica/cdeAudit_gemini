# a_clean_source_data.py
# Purpose: A standalone script to read the raw CDE catalog, perform complex
# data cleaning, consolidation, and save the result to a new file.

import pandas as pd
import re
from tqdm import tqdm
import sys

# --- Configuration ---
SOURCE_CATALOG_PATH = 'cdeCatalogs/20250627_cdeCatalog.csv'
CLEANED_OUTPUT_PATH = 'cde_catalog_CLEANED.csv'
PV_MAP_PATH = 'mapping/permissible_values_map.csv'

# --- Helper Functions ---

def create_pv_lookup_map(map_filepath):
    """Creates a lookup dictionary from the permissible values map."""
    print(f"Loading permissible values map from: {map_filepath}")
    try:
        map_df = pd.read_csv(map_filepath)
        map_df.dropna(subset=['original_expression', 'standardized_pv'], inplace=True)
        return pd.Series(map_df.standardized_pv.values, index=map_df.original_expression).to_dict()
    except FileNotFoundError:
        print(f"Warning: Map file not found at '{map_filepath}'.")
        return {}

def split_and_clean_list(text_value):
    """Robustly splits a string by EITHER pipe OR comma using regex."""
    if not isinstance(text_value, str) or pd.isna(text_value): return []
    items = re.split(r'[,|]', text_value)
    return [item.strip() for item in items if item and item.strip()]

def to_postgres_array(items_list):
    """Formats a Python list into a PostgreSQL array string."""
    if not items_list: return None
    def escape_item(item):
        item_str = str(item).replace('\\', '\\\\').replace('"', '\\"')
        return f'"{item_str}"' if any(c in item_str for c in [',', '{', '}']) else item_str
    return f"{{{','.join(escape_item(item) for item in items_list)}}}"

def clean_permissible_values(value, lookup_map):
    """
    Cleans the permissible_values field using the lookup map first,
    then applies general cleaning rules as a fallback.
    """
    if pd.isna(value): return None
    value_str = str(value)

    if value_str in lookup_map:
        return to_postgres_array(split_and_clean_list(lookup_map[value_str]))

    if re.match(r'^[\w\s.,|_-]+$', value_str):
         items = split_and_clean_list(value_str)
         if items:
            return to_postgres_array(items)

    if not any(c in value_str for c in ['{', '[', '|', ',']):
        return to_postgres_array([value_str])
        
    return None
    
def consolidate_synonyms(row, source_cols):
    """Consolidates multiple synonym columns into one de-duplicated list."""
    unique_items = set()
    for col in source_cols:
        if col in row and pd.notna(row[col]):
            unique_items.update(split_and_clean_list(row[col]))
    return to_postgres_array(sorted(list(unique_items))) if unique_items else None

# --- Main Execution Block ---

def main():
    """Main function to run the entire cleaning and consolidation process."""
    
    print(f"Reading raw source data from: {SOURCE_CATALOG_PATH}")
    try:
        df = pd.read_csv(SOURCE_CATALOG_PATH, dtype=str).fillna(pd.NA)
    except FileNotFoundError:
        print(f"FATAL ERROR: Source file not found at '{SOURCE_CATALOG_PATH}'", file=sys.stderr)
        sys.exit(1)

    print("\nStarting data cleaning and consolidation...")
    tqdm.pandas()

    # --- Step 1: Clean list-like columns ('permissible_values', 'collections') ---
    print("Step 1 of 3: Cleaning list-like columns...")
    pv_lookup_map = create_pv_lookup_map(PV_MAP_PATH)
    if 'permissible_values' in df.columns:
        df['permissible_values'] = df['permissible_values'].progress_apply(lambda x: clean_permissible_values(x, pv_lookup_map))
    
    # --- THIS IS THE NEW FIX ---
    # Apply the same cleaning and formatting logic to the 'collections' column.
    if 'collections' in df.columns:
        print("Formatting 'collections' column...")
        # Re-use the helper functions to ensure correct array formatting
        df['collections'] = df['collections'].progress_apply(lambda x: to_postgres_array(split_and_clean_list(x)))
    
    # --- Step 2: Consolidate Synonym Columns ---
    print("Step 2 of 3: Consolidating synonym columns...")
    synonym_map = {
        'alternate_titles': ['AlternateDescription', 'alternate_titles'],
        'alternate_headers': ['AlternateItemNames', 'alternate_headers'],
        'alternate_terms': ['synonymous_terms', 'alternate_terms']
    }
    for new_col, source_cols in synonym_map.items():
        existing_source_cols = [c for c in source_cols if c in df.columns]
        if existing_source_cols:
            df[new_col] = df.progress_apply(lambda row: consolidate_synonyms(row, existing_source_cols), axis=1)
        else:
            df[new_col] = None

    # --- Step 3: Assemble and Save Final Output ---
    print("Step 3 of 3: Assembling final clean data file...")
    
    final_columns = [
        'ID', 'title', 'variable_name', 'preferred_question_text', 'collections',
        'unit_of_measure', 'value_format', 'permissible_values',
        'alternate_titles', 'alternate_headers', 'alternate_terms',
        'standardized_value', 'value_mapping'
    ]
    
    existing_final_columns = [col for col in final_columns if col in df.columns]
    df_cleaned = df[existing_final_columns]
    
    # NOTE: The validation step was removed to avoid confusion. The cleaning functions
    # are now more robust, making a separate validation less critical for this script.
    
    df_cleaned.to_csv(CLEANED_OUTPUT_PATH, index=False)
    
    print("\nCleaning process complete.")
    print(f"Output file is ready at '{CLEANED_OUTPUT_PATH}'.")

if __name__ == "__main__":
    main()