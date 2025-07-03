# audit_catalog.py
# Purpose: A standalone utility script to perform a final audit on the
# polished CDE catalog and report on data completeness and missingness.

import os
import pandas as pd
import logging
from collections import Counter

# --- CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Input Path for the final, polished catalog
CATALOG_PATH = os.path.join('outputs', 'final_enriched', 'cde_catalog_enriched.csv')

# Columns to check for general missingness
MISSINGNESS_COLS = ['alternate_titles', 'alternate_headers', 'synonymous_terms']


def is_missing(value):
    """Checks if a value is null, NaN, or an empty/whitespace string."""
    return pd.isna(value) or (isinstance(value, str) and not value.strip())


def audit_catalog():
    """
    Main function to run the audit process and print a summary report.
    """
    logging.info("--- Starting Final Catalog Audit ---")

    # 1. Load the polished catalog
    if not os.path.exists(CATALOG_PATH):
        logging.error(f"Polished catalog not found at '{CATALOG_PATH}'. Aborting.")
        return
    
    try:
        df = pd.read_csv(CATALOG_PATH, dtype=str, low_memory=False)
        total_rows = len(df)
        logging.info(f"Successfully loaded {total_rows:,} CDEs from '{CATALOG_PATH}'.")
    except Exception as e:
        logging.error(f"Could not read the catalog file. Error: {e}")
        return

    # 2. Audit 'permissible_values' and 'value_mapping'
    logging.info("Auditing 'permissible_values' and 'value_mapping' fields...")
    
    # Find rows where permissible_values is missing
    pv_is_missing_mask = df['permissible_values'].apply(is_missing)
    null_pv_count = pv_is_missing_mask.sum()
    
    # Of those rows, find where value_mapping is NOT missing
    df_null_pv = df[pv_is_missing_mask]
    vm_is_populated_mask = ~df_null_pv['value_mapping'].apply(is_missing)
    vm_populated_count = vm_is_populated_mask.sum()

    # --- NEW: Identify collections with the most null PVs ---
    logging.info("Identifying collections most impacted by null 'permissible_values'...")
    null_pv_collections = df_null_pv['collections'].dropna().str.split('|').explode()
    collection_null_counts = null_pv_collections.value_counts()
    
    # 3. Audit general missingness for other specified columns
    logging.info("Auditing other specified text fields for missingness...")
    missingness_report = {}
    for col in MISSINGNESS_COLS:
        if col in df.columns:
            missing_count = df[col].apply(is_missing).sum()
            missing_percentage = (missing_count / total_rows) * 100 if total_rows > 0 else 0
            missingness_report[col] = {
                "count": missing_count,
                "percentage": f"{missing_percentage:.2f}%"
            }
        else:
            missingness_report[col] = "Column not found"

    # 4. Print the final report
    print("\n" + "="*50)
    print("--- CDE Catalog Audit Report ---")
    print("="*50)
    
    print("\nValue Definition Audit:")
    print(f"  - Total CDEs with null 'permissible_values': {null_pv_count:,} / {total_rows:,}")
    if null_pv_count > 0:
        pv_missing_percentage = (null_pv_count / total_rows) * 100
        print(f"    ({pv_missing_percentage:.2f}% of total CDEs)")
    
    print(f"  - CDEs with null 'permissible_values' BUT populated 'value_mapping': {vm_populated_count:,}")
    if null_pv_count > 0:
        fallback_percentage = (vm_populated_count / null_pv_count) * 100
        print(f"    ({fallback_percentage:.2f}% of null PV CDEs have a fallback)")

    print("\nField Completeness Audit:")
    for col, stats in missingness_report.items():
        if isinstance(stats, dict):
            print(f"  - Missing '{col}': {stats['count']:,} ({stats['percentage']})")
        else:
            print(f"  - Column '{col}': {stats}")
    
    # --- NEW: Display the top collections with null PVs ---
    print("\nTop 10 Collections with Null 'permissible_values':")
    if not collection_null_counts.empty:
        for i, (collection, count) in enumerate(collection_null_counts.head(10).items()):
            print(f"  {i+1}. {collection}: {count:,} CDEs")
    else:
        print("  - No collections found for CDEs with null permissible values.")
            
    print("\n" + "="*50)
    print("--- End of Report ---")


if __name__ == "__main__":
    audit_catalog()
