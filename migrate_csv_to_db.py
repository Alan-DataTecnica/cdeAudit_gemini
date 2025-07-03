# migrate_csv_to_db.py (Final Version with Intelligent Classification)

import pandas as pd
import psycopg2
import os
import re
import json
import sqlite3
from io import StringIO
import sys
from dotenv import load_dotenv
from tqdm import tqdm

# --- Configuration ---
CDE_CATALOG_PATH = 'cdeCatalogs/20250627_cdeCatalog.csv'
DEDUPLICATION_REPORT_PATH = "deduplication_report.csv"
BACKUP_CATALOG_PATH = 'cdeCatalogs/backupOG_cdeCatalog.csv'
WEBLINKS_SQLITE_PATH = 'cdeCatalogs/20250603_2030_cde.sqlite'
SUGGESTIONS_DIR = 'outputs/stage_3/raw_responses'


# --- Helper Functions ---

def load_ai_suggestions(suggestions_dir):
    """
    Loads and processes all raw AI suggestion files from a directory.
    This logic is adapted from the main application's data_loader.
    """
    print("\nLoading all AI suggestions for context...")
    all_suggestions, failed_files = {}, []
    if not os.path.exists(suggestions_dir):
        print(f"Warning: Suggestions directory not found at {suggestions_dir}. Skipping AI suggestions.")
        return {}
    
    files_to_process = [f for f in os.listdir(suggestions_dir) if f.endswith(('.json', '.txt'))]
    for filename in tqdm(files_to_process, desc="Loading AI Suggestions"):
        filepath = os.path.join(suggestions_dir, filename)
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
                data, suggestions_list = json.loads(content), []
                if isinstance(data, dict) and "candidates" in data:
                    text_content = data.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
                    if text_content: suggestions_list = json.loads(text_content)
                elif isinstance(data, list):
                    suggestions_list = data
                for sug in suggestions_list:
                    if cde_id := sug.get("ID"):
                        all_suggestions[str(cde_id)] = sug.get("suggestions", {})
        except Exception:
            # For this script, we can ignore files that fail to parse
            pass
    print(f"Loaded {len(all_suggestions)} AI suggestions.")
    return all_suggestions

def safe_json_loads(s):
    """Safely loads a JSON string, returning None if it fails."""
    try:
        return json.loads(s) if pd.notna(s) else None
    except (json.JSONDecodeError, TypeError):
        return None

def classify_value_format(row, ai_suggestions):
    """
    Determines the value_format using a waterfall logic of priorities.
    """
    # Priority 1: Use existing value if it's not empty
    if pd.notna(row['value_format']):
        return row['value_format']

    # Priority 2: Check for an AI suggestion
    cde_id = str(row['ID'])
    if cde_id in ai_suggestions and 'value_format' in ai_suggestions[cde_id]:
        return ai_suggestions[cde_id]['value_format']

    # Priority 3: Parse from JSON columns (standardized_value has priority)
    for col in ['standardized_value', 'value_mapping']:
        data = safe_json_loads(row.get(col))
        if data and isinstance(data, dict) and 'value_format' in data:
            return data['value_format']

    # Priority 4: Apply heuristics based on permissible_values
    if pd.notna(row['permissible_values']):
        pv_str = str(row['permissible_values']).lower()
        if pv_str in ["{true,false}", "{yes,no}"]:
            return 'binary'
        # Simple check if it looks like a list of non-numeric categories
        if '{' in pv_str and any(c.isalpha() for c in pv_str):
            return 'categorical'

    # Priority 5: Default if no other rule applies
    return 'free_text'

def get_values_from_json(row, ai_suggestions):
    """
    Extracts min, max, and permissible values based on the classified format.
    """
    value_format = row['classified_format']
    min_val, max_val, perm_vals = None, None, None

    # Source the canonical JSON object first
    json_obj = safe_json_loads(row['standardized_value']) or safe_json_loads(row.get('value_mapping'))
    
    if json_obj:
        if value_format == 'range':
            min_val = pd.to_numeric(json_obj.get('min'), errors='coerce')
            max_val = pd.to_numeric(json_obj.get('max'), errors='coerce')
        elif value_format in ['categorical', 'binary', 'numerical_list']:
            raw_list = json_obj.get('options') or json_obj.get('values') or json_obj.get('permissible_values')
            if raw_list and isinstance(raw_list, list):
                clean_values = [str(v).replace('\\', '\\\\').replace('"', '\\"').replace('{', '\\{').replace('}', '\\}') for v in raw_list]
                perm_vals = f"{{{','.join(clean_values)}}}"

    # If permissible values weren't found in a JSON object, try the raw column
    if perm_vals is None and pd.notna(row['permissible_values']):
        raw_list = [v.strip() for v in str(row['permissible_values']).strip('{}').split(',')]
        clean_values = [str(v).replace('\\', '\\\\').replace('"', '\\"').replace('{', '\\{').replace('}', '\\}') for v in raw_list]
        perm_vals = f"{{{','.join(clean_values)}}}"
    
    return min_val, max_val, perm_vals


# --- Migration Functions ---

def migrate_cde_catalog(conn, ai_suggestions):
    """
    Reads, parses, enriches, and loads the CDE catalog data.
    """
    print("--- Starting CDE Catalog Migration ---")
    
    columns_to_read = [
        "ID", "title", "variable_name", "permissible_values", "unit_of_measure",
        "value_format", "standardized_value", "value_mapping"
    ]
    final_db_columns = [
        "ID", "title", "variable_name", "permissible_values", "unit_of_measure",
        "value_format", "min_value", "max_value", "version"
    ]
    
    try:
        df = pd.read_csv(CDE_CATALOG_PATH, usecols=lambda c: c in columns_to_read, dtype={'ID': str})
        backup_df = pd.read_csv(BACKUP_CATALOG_PATH, usecols=['ID', 'min_value', 'max_value'], dtype={'ID': str}).set_index('ID')

        print("Enriching data: Classifying formats and parsing values...")
        df['classified_format'] = df.apply(lambda row: classify_value_format(row, ai_suggestions), axis=1)
        
        parsed_values = df.apply(lambda row: get_values_from_json(row, ai_suggestions), axis=1, result_type='expand')
        df[['parsed_min', 'parsed_max', 'parsed_permissible']] = parsed_values

        df['min_value'] = df['parsed_min'].fillna(df['ID'].map(backup_df['min_value']))
        df['max_value'] = df['parsed_max'].fillna(df['ID'].map(backup_df['max_value']))
        df['permissible_values'] = df['parsed_permissible']
        # The value_format is now the one we classified
        df['value_format'] = df['classified_format']
        df['version'] = '1.0'
        
        df_final = df[final_db_columns]
        
        buffer = StringIO()
        df_final.to_csv(buffer, index=False, header=False, sep='\t')
        buffer.seek(0)
        table_name = 'public.cde_catalog'
        columns_sql = '", "'.join(df_final.columns)
        
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE {table_name} CASCADE;")
            cur.copy_expert(f'COPY {table_name} ("{columns_sql}") FROM STDIN WITH CSV DELIMITER E\'\\t\'', buffer)
        
        conn.commit()
        print("CDE Catalog migration successful!")

    except Exception as e:
        print(f"An error occurred during CDE Catalog migration: {e}", file=sys.stderr)
        conn.rollback()
        raise

# ... [migrate_weblinks and migrate_icd_codes functions remain the same as the previous version] ...

def migrate_weblinks(conn):
    """
    Reads CDE web links from a SQLite database and populates the cde_references table.
    """
    print("\n--- Starting Web Link Migration ---")
    try:
        sqlite_conn = sqlite3.connect(WEBLINKS_SQLITE_PATH)
        links_df = pd.read_sql_query("SELECT ID, web_link FROM cdes WHERE web_link IS NOT NULL", sqlite_conn)
        sqlite_conn.close()

        records_to_insert = links_df.to_records(index=False)
        if len(records_to_insert) == 0:
            print("No web links found to insert.")
            return

        print(f"Found {len(records_to_insert)} web links to migrate.")
        with conn.cursor() as cur:
            insert_query = "INSERT INTO public.cde_references (cde_id, source_url) VALUES (%s, %s) ON CONFLICT DO NOTHING;"
            cur.executemany(insert_query, records_to_insert)
            print(f"Inserted {cur.rowcount} new web link references.")
        conn.commit()
    except Exception as e:
        print(f"An error occurred during web link migration: {e}")
        conn.rollback()
        raise

def migrate_icd_codes(conn):
    """
    Parses the deduplication report to populate the cde_external_codes table.
    """
    print("\n--- Starting ICD Code Migration ---")
    try:
        cde_df = pd.read_sql_query('SELECT "ID", title FROM public.cde_catalog', conn)
        cde_df.rename(columns={'ID': 'cde_id'}, inplace=True)
        report_df = pd.read_csv(DEDUPLICATION_REPORT_PATH)

        def extract_icd_code(title):
            if pd.isna(title): return None, None
            match = re.search(r'\((ICD-?\d{1,2}-?[A-Z]{0,2}),\s*([A-Z0-9\.]+)\)', str(title), re.IGNORECASE)
            if match:
                return match.group(1).upper().replace('ICD', 'ICD-'), match.group(2).strip()
            return None, None

        report_df[['code_system', 'code_value']] = report_df['Original_Title'].apply(lambda x: pd.Series(extract_icd_code(x)))
        original_map = report_df[['Original_Title', 'code_system', 'code_value']].rename(columns={'Original_Title': 'title'})
        duplicate_map = report_df[['Duplicate_Title', 'code_system', 'code_value']].rename(columns={'Duplicate_Title': 'title'})
        full_map = pd.concat([original_map, duplicate_map]).dropna(subset=['title', 'code_value']).drop_duplicates()

        merged_df = pd.merge(cde_df, full_map, on='title', how='inner')
        records_to_insert = merged_df[['cde_id', 'code_system', 'code_value']].drop_duplicates().to_records(index=False)
        
        if len(records_to_insert) > 0:
            with conn.cursor() as cur:
                insert_query = "INSERT INTO public.cde_external_codes (cde_id, code_system, code_value) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING;"
                cur.executemany(insert_query, records_to_insert)
                print(f"Inserted {cur.rowcount} new ICD code mappings.")
            conn.commit()
        else:
            print("No matching ICD codes found to insert.")
    except Exception as e:
        print(f"An error occurred during ICD code migration: {e}")
        conn.rollback()
        raise

def main():
    """Main function to orchestrate the migration steps."""
    load_dotenv()
    DATABASE_URI = os.getenv("DATABASE_URI")
    if not DATABASE_URI:
        print("FATAL ERROR: DATABASE_URI not found.", file=sys.stderr)
        sys.exit(1)

    conn = None
    try:
        # Pre-load AI suggestions so we can pass them to the catalog migration
        ai_suggestions = load_ai_suggestions(SUGGESTIONS_DIR)
        
        conn = psycopg2.connect(DATABASE_URI)
        print("Database connection successful.")
        
        migrate_cde_catalog(conn, ai_suggestions)
        migrate_weblinks(conn)
        migrate_icd_codes(conn)
        
        print("\nAll migration tasks completed successfully!")

    except Exception as e:
        print(f"\nFATAL: A failure occurred during the migration process: {e}", file=sys.stderr)
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")


if __name__ == "__main__":
    main()