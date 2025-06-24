import csv
import logging
import os

# --- CONFIGURATION ---
TARGET_FILE = os.path.join('cdeCatalogs', 'cdeCatalog.csv')

# --- LOGGING SETUP ---
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def main():
    """
    Reads the target CSV line-by-line to identify parsing errors.
    """
    if not os.path.exists(TARGET_FILE):
        logging.error(f"File not found: {TARGET_FILE}")
        return

    logging.info(f"Starting diagnostic read of: {TARGET_FILE}")
    line_count = 0
    try:
        with open(TARGET_FILE, 'r', encoding='utf-8') as infile:
            # Use the csv reader which is more explicit about errors
            reader = csv.reader(infile)
            for row in reader:
                line_count += 1
        
        # If the loop completes without error
        logging.info(f"SUCCESS: Successfully parsed all {line_count} lines without errors.")

    except csv.Error as e:
        # If the CSV module itself finds a structural error
        logging.error("--- PARSING FAILURE DETECTED ---")
        logging.error(f"A structural error was found at or near line: {line_count + 1}")
        logging.error(f"Error details: {e}")
        logging.error("The pipeline cannot proceed until this formatting issue in 'cde_catalog_processed.csv' is resolved.")
    except Exception as e:
        logging.error(f"An unexpected error occurred at line {line_count + 1}: {e}")

if __name__ == "__main__":
    main()