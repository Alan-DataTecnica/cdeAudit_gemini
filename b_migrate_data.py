# b_migrate_data.py
# Purpose: A standalone script to migrate all CLEANED and other source data
# into their final PostgreSQL database tables in reliable batches.

import pandas as pd
import psycopg2
import os
import re
import sqlite3
from io import StringIO
import sys
from dotenv import load_dotenv
from tqdm import tqdm

# --- Configuration ---
CLEANED_CATALOG_PATH = 'cde_catalog_CLEANED.csv'
DEDUPLICATION_REPORT_PATH = "deduplication_report.csv"
WEBLINKS_SQLITE_PATH = 'cdeCatalogs/20250603_2030_cde.sqlite'
ORIGINAL_CATALOG_PATH = 'cdeCatalogs/20250627_cdeCatalog.csv' # For SNOMED, etc.
BATCH_SIZE = 500 # Process 10,000 rows at a time

# --- Migration Functions ---

def migrate_cde_catalog(conn):
    """
    Reads the cleaned CDE catalog and bulk-inserts it into the final table
    using manageable batches to ensure stability.
    """
    print("--- Migrating Main CDE Catalog from CLEANED file ---")
    
    db_columns = [
        "ID", "title", "variable_name", "permissible_values", "unit_of_measure",
        "value_format", "preferred_question_text", "collections", "min_value", "max_value", "version"
    ]
    
    try:
        df = pd.read_csv(CLEANED_CATALOG_PATH, dtype=str)
        print(f"Read {len(df)} cleaned rows from {CLEANED_CATALOG_PATH}")

        cols_to_load = [col for col in db_columns if col in df.columns]
        df_final = df[cols_to_load]
        
        table_name = 'public.cde_catalog'
        columns_sql = '", "'.join(cols_to_load)

        with conn.cursor() as cur:
            print(f"Clearing existing data from {table_name}...")
            cur.execute(f"TRUNCATE {table_name} CASCADE;")
            
            print(f"Preparing to insert {len(df_final)} rows in batches of {BATCH_SIZE}...")
            
            for i in tqdm(range(0, len(df_final), BATCH_SIZE), desc="Migrating CDE Catalog"):
                chunk = df_final.iloc[i:i + BATCH_SIZE]
                
                buffer = StringIO()
                chunk.to_csv(buffer, index=False, header=False, sep='\t')
                buffer.seek(0)
                
                cur.copy_expert(f'COPY {table_name} ("{columns_sql}") FROM STDIN WITH CSV DELIMITER E\'\\t\'', buffer)
                conn.commit()

        print("CDE Catalog migration successful!")

    except Exception as e:
        print(f"An error occurred during CDE Catalog migration: {e}", file=sys.stderr)
        conn.rollback()
        raise

def migrate_synonyms(conn):
    """
    Reads the consolidated synonym columns from the cleaned data and populates
    the cde_synonyms table in batches.
    """
    print("\n--- Migrating Synonyms ---")
    try:
        synonym_cols = ['ID', 'alternate_titles', 'alternate_headers', 'alternate_terms']
        df = pd.read_csv(CLEANED_CATALOG_PATH, usecols=lambda c: c in synonym_cols, dtype=str)
        df.dropna(subset=synonym_cols[1:], how='all', inplace=True)

        records_to_insert = []
        type_map = {'alternate_titles': 'alternate_title', 'alternate_headers': 'alternate_header', 'alternate_terms': 'alternate_term'}

        for _, row in tqdm(df.iterrows(), total=df.shape[0], desc="Processing Synonyms"):
            cde_id = row['ID']
            for col_name, synonym_type in type_map.items():
                if col_name in row and pd.notna(row[col_name]):
                    # Un-nest the PostgreSQL array string format, e.g., {syn1,"syn,2"}
                    synonyms = re.findall(r'(?:"([^"]*)"|([^,{}]+))', str(row[col_name]))
                    for s in synonyms:
                        # re.findall with groups returns tuples, e.g., ('', 'syn1'), ('syn,2', '')
                        synonym_clean = s[0] if s[0] else s[1]
                        if synonym_clean:
                            records_to_insert.append((cde_id, synonym_clean.strip(), synonym_type))
        
        if not records_to_insert:
            print("No consolidated synonyms found to insert.")
            return

        with conn.cursor() as cur:
            insert_query = "INSERT INTO public.cde_synonyms (cde_id, synonym_text, synonym_type) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING;"
            
            total_inserted = 0
            for i in tqdm(range(0, len(records_to_insert), BATCH_SIZE), desc="Migrating Synonyms"):
                chunk = records_to_insert[i : i + BATCH_SIZE]
                cur.executemany(insert_query, chunk)
                total_inserted += cur.rowcount
                conn.commit()
            print(f"Inserted {total_inserted} new synonym mappings.")
            
    except Exception as e:
        print(f"An error occurred during synonym migration: {e}", file=sys.stderr)
        conn.rollback()
        raise

def migrate_external_codes(conn):
    """Migrates both ICD and SNOMED codes into the cde_external_codes table."""
    print("\n--- Migrating External Codes (ICD & SNOMED) ---")
    
    # ICD Code Logic
    try:
        cde_df = pd.read_sql_query('SELECT "ID", title FROM public.cde_catalog', conn)
        cde_df.rename(columns={'ID': 'cde_id'}, inplace=True)
        report_df = pd.read_csv(DEDUPLICATION_REPORT_PATH)

        def extract_icd_code(title):
            if pd.isna(title): return None, None
            match = re.search(r'\((ICD-?\d{1,2}-?[A-Z]{0,2}),\s*([A-Z0-9\.]+)\)', str(title), re.IGNORECASE)
            if match: return match.group(1).upper().replace('ICD', 'ICD-'), match.group(2).strip()
            return None, None

        report_df[['code_system', 'code_value']] = report_df['Original_Title'].apply(lambda x: pd.Series(extract_icd_code(x)))
        original_map = report_df[['Original_Title', 'code_system', 'code_value']].rename(columns={'Original_Title': 'title'})
        duplicate_map = report_df[['Duplicate_Title', 'code_system', 'code_value']].rename(columns={'Duplicate_Title': 'title'})
        full_map = pd.concat([original_map, duplicate_map]).dropna(subset=['title', 'code_value']).drop_duplicates()
        merged_df = pd.merge(cde_df, full_map, on='title', how='inner')
        icd_records = merged_df[['cde_id', 'code_system', 'code_value']].drop_duplicates().to_records(index=False)
        
        if len(icd_records) > 0:
            with conn.cursor() as cur:
                query = "INSERT INTO public.cde_external_codes (cde_id, code_system, code_value) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING;"
                cur.executemany(query, icd_records)
                print(f"Inserted {cur.rowcount} new ICD code mappings.")
            conn.commit()
        else:
            print("No matching ICD codes found to insert.")
    except Exception as e:
        print(f"An error occurred during ICD code migration: {e}", file=sys.stderr)
        conn.rollback()
        raise

    # SNOMED Code Logic
    try:
        snomed_cols = ['ID', 'snomed_code', 'snomed_alias']
        df_snomed = pd.read_csv(ORIGINAL_CATALOG_PATH, usecols=lambda c: c in snomed_cols, dtype=str)
        df_snomed.dropna(subset=['snomed_code'], inplace=True)
        snomed_records = []
        for _, row in df_snomed.iterrows():
            snomed_records.append((row['ID'], 'SNOMED-CT', row['snomed_code'], row.get('snomed_alias')))
        
        if len(snomed_records) > 0:
            with conn.cursor() as cur:
                query = "INSERT INTO public.cde_external_codes (cde_id, code_system, code_value, code_description) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;"
                cur.executemany(query, snomed_records)
                print(f"Inserted {cur.rowcount} new SNOMED code mappings.")
            conn.commit()
        else:
            print("No SNOMED codes found to insert.")
    except Exception as e:
        print(f"An error occurred during SNOMED code migration: {e}", file=sys.stderr)
        conn.rollback()
        raise

def migrate_weblinks(conn):
    """Reads CDE web links from a SQLite database."""
    print("\n--- Migrating Web Links ---")
    try:
        with sqlite3.connect(WEBLINKS_SQLITE_PATH) as sqlite_conn:
            links_df = pd.read_sql_query("SELECT ID, web_link FROM cdes WHERE web_link IS NOT NULL", sqlite_conn)
        
        records_to_insert = links_df.to_records(index=False)
        if len(records_to_insert) == 0: return print("No web links found to insert.")

        with conn.cursor() as cur:
            query = "INSERT INTO public.cde_references (cde_id, source_url) VALUES (%s, %s) ON CONFLICT DO NOTHING;"
            cur.executemany(query, records_to_insert)
            print(f"Inserted {cur.rowcount} new web link references.")
        conn.commit()
    except Exception as e:
        print(f"An error occurred during web link migration: {e}", file=sys.stderr)
        conn.rollback()
        raise

# --- Main Execution Block ---

def main():
    """Main function to orchestrate the migration steps."""
    load_dotenv()
    
    db_host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASSWORD")

    if not all([db_host, db_name, db_user, db_pass]):
        print("FATAL ERROR: One or more database connection variables are not set in the .env file.", file=sys.stderr)
        sys.exit(1)

    conn = None
    try:
        conn = psycopg2.connect(host=db_host, dbname=db_name, user=db_user, password=db_pass)
        print("Database connection successful.")
        
        # Run migrations in a logical order
        migrate_cde_catalog(conn)
        migrate_synonyms(conn)
        migrate_external_codes(conn)
        migrate_weblinks(conn)
        
        print("\n✅ All migration tasks completed successfully!")

    except Exception as e:
        print(f"\nFATAL: A failure occurred during the migration process: {e}", file=sys.stderr)
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")


if __name__ == "__main__":
    main()