import logging
import os

# --- CONFIGURATION ---
TARGET_FILE = os.path.join('outputs', 'stage_1', 'cde_catalog_processed.csv')

# --- LOGGING SETUP ---
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def main():
    """
    Reads the target file line-by-line as raw text to verify its integrity
    and get a true line count.
    """
    if not os.path.exists(TARGET_FILE):
        logging.error(f"File not found: {TARGET_FILE}")
        return

    logging.info(f"Starting low-level integrity check of: {TARGET_FILE}")
    line_count = 0
    try:
        with open(TARGET_FILE, 'r', encoding='utf-8', errors='strict') as infile:
            for line in infile:
                line_count += 1
        
        # If the loop completes without error
        logging.info(f"SUCCESS: The script was able to read all {line_count:,} lines without encountering encoding errors.")

    except UnicodeDecodeError as e:
        logging.error("--- UNICODE DECODE FAILURE DETECTED ---")
        logging.error(f"A critical encoding error was found at or near line: {line_count + 1}")
        logging.error(f"This is often caused by a non-UTF8 character or a null byte ('\\x00') in the file.")
        logging.error(f"Error details: {e}")
        logging.error("The file cannot be reliably processed until this character is removed.")
    except Exception as e:
        logging.error(f"An unexpected error occurred at line {line_count + 1}: {e}")

if __name__ == "__main__":
    main()