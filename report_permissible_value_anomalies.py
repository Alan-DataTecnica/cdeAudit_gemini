# PREPARATION FOR NEON POSTGRES UPLOAD JUNE 30TH 2025

# report_permissible_value_anomalies.py
# Purpose: A utility script to scan the CDE catalog and identify all unique,
# non-standard entries in the 'permissible_values' column for manual review.

import pandas as pd
import os

# --- Configuration ---
SOURCE_CATALOG_PATH = 'cdeCatalogs/20250627_cdeCatalog.csv'
ANOMALY_REPORT_PATH = 'permissible_values_anomalies.txt'

def generate_anomaly_report():
    """
    Finds and reports on unique, malformed 'permissible_values' entries.
    """
    print(f"Reading source file: {SOURCE_CATALOG_PATH}")
    
    try:
        # We only need to read the one column for this task, which is efficient.
        df = pd.read_csv(SOURCE_CATALOG_PATH, usecols=['permissible_values'], dtype=str)
    except FileNotFoundError:
        print(f"FATAL ERROR: Source file not found at '{SOURCE_CATALOG_PATH}'")
        return
    except ValueError:
        print(f"FATAL ERROR: The source CSV does not contain a 'permissible_values' column.")
        return

    # Drop any rows where 'permissible_values' is empty
    pv_series = df['permissible_values'].dropna()

    # Define the filter logic: Find any string that does NOT start with '[' or '{'.
    # This identifies values that are not in a JSON list or object format.
    is_anomaly = ~pv_series.str.startswith(('[', '{'), na=False)
    anomalous_values = pv_series[is_anomaly]

    # Get the unique set of these anomalous values
    unique_anomalies = anomalous_values.unique()

    if len(unique_anomalies) == 0:
        print("No anomalous 'permissible_values' formats were found. All entries appear to be standard.")
        return

    print(f"Found {len(unique_anomalies)} unique non-standard formats.")
    print(f"Saving report to: {ANOMALY_REPORT_PATH}")

    # Write the unique anomalies to the output text file, one per line.
    with open(ANOMALY_REPORT_PATH, 'w', encoding='utf-8') as f:
        for item in sorted(unique_anomalies):
            f.write(f"{item}\n")

    print("\n✅ Anomaly report generation complete!")
    print("Please review the report to define the necessary hard-coded conversions.")

if __name__ == "__main__":
    generate_anomaly_report()