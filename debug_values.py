import pandas as pd
import re
import os
import sys
import logging

# --- Basic Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- CONFIGURATION ---
INPUT_DIR = "cdeCatalogs"
INPUT_FILENAME = "cdeCatalog.csv" # <-- Make sure this is your CSV file name
OUTPUT_DIR = "outputs/debug"
DIAGNOSTIC_FILENAME = "diagnostic_format_report.csv"
# --- End of Configuration ---

def diagnose_formats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Analyzes the 'Values' column and generates a report on which patterns match.
    """
    logging.info("Starting format diagnosis...")
    
    # Define all patterns we intend to handle
    patterns = {
        'placeholder': re.compile(r'^(free text|numeric|integer|float|text|boolean)$', re.IGNORECASE),
        'key_value_meta': re.compile(r'min:.*max:', re.IGNORECASE),
        'ordinal_scale': re.compile(r'(\d+:\s*[\w\s]+)', re.IGNORECASE),
        'interval_simple': re.compile(r'^\s*(\d+\.?\d*)\s*-\s*(\d+\.?\d*)\s*$'),
        'json_array': re.compile(r'^\s*\[.*\]\s*$'),
        'pipe_separated': re.compile(r'.*\|.*'),
        'constraint_expression': re.compile(r'^\(y>=.*\)$', re.IGNORECASE)
    }
    
    results = []
    
    unique_values = df['Values'].astype(str).dropna().unique()
    logging.info(f"Analyzing {len(unique_values)} unique 'Values' entries...")

    for value in unique_values:
        matched_pattern = "Unmatched" # Default
        for name, pattern in patterns.items():
            if pattern.search(value):
                matched_pattern = name
                break # Stop after first match
        
        results.append({'value': value, 'matched_pattern': matched_pattern})
        
    return pd.DataFrame(results)

def main():
    """Main function to run the diagnostic process."""
    input_path = os.path.join(INPUT_DIR, INPUT_FILENAME)
    output_path = os.path.join(OUTPUT_DIR, DIAGNOSTIC_FILENAME)
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    try:
        logging.info(f"Loading CDE catalog from: {input_path}")
        df_cde = pd.read_csv(input_path, sep=',', engine='python', on_bad_lines='warn')
    except FileNotFoundError:
        logging.error(f"Fatal: Input file not found at '{input_path}'")
        sys.exit(1)

    diagnostic_df = diagnose_formats(df_cde)

    logging.info(f"Saving diagnostic report to: {output_path}")
    diagnostic_df.to_csv(output_path, index=False)
    
    unmatched_count = diagnostic_df[diagnostic_df['matched_pattern'] == 'Unmatched'].shape[0]
    logging.info(f"Diagnosis complete. Found {unmatched_count} unique formats that were not matched by any pattern.")
    logging.info("Please review the diagnostic report to identify patterns we need to add or fix.")

if __name__ == "__main__":
    main()