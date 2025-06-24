import pandas as pd
import logging
import os

# --- CONFIGURATION ---
TARGET_FILE = os.path.join('outputs', 'stage_1', 'cde_catalog_processed.csv')
CHUNK_SIZE = 10000 # Read the file in chunks of 10,000 rows

# --- LOGGING SETUP ---
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def main():
    """
    Reads the target CSV in chunks using pandas to isolate parsing errors.
    """
    if not os.path.exists(TARGET_FILE):
        logging.error(f"File not found: {TARGET_FILE}")
        return

    logging.info(f"Starting pandas chunk-based read of: {TARGET_FILE}")
    total_rows_processed = 0
    chunk_num = 0

    try:
        # Create a TextFileReader iterator to read the file in chunks
        with pd.read_csv(
            TARGET_FILE,
            engine='python',    # Use the flexible python engine
            chunksize=CHUNK_SIZE,
            on_bad_lines='warn' # Warn on bad lines but continue
        ) as reader:
            for chunk in reader:
                chunk_rows = len(chunk)
                start_row = total_rows_processed + 1
                end_row = total_rows_processed + chunk_rows
                logging.info(f"Successfully processed chunk {chunk_num} (rows {start_row:,} to {end_row:,})...")
                total_rows_processed += chunk_rows
                chunk_num += 1

        logging.info("--- DIAGNOSTIC COMPLETE ---")
        logging.info(f"SUCCESS: Pandas successfully processed a total of {total_rows_processed:,} rows.")

    except Exception as e:
        logging.error("--- PANDAS PARSING FAILURE DETECTED ---")
        logging.error(f"The error occurred while processing the chunk starting at line: {total_rows_processed + 1:,}")
        logging.error(f"Error details: {e}")

if __name__ == "__main__":
    main()