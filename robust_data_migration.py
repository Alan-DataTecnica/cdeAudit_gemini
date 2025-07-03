# b_migrate_data.py
# FINAL SOLUTION: Multi-chunk strategy with fresh connections and aggressive rest periods
# Strategy: Split large operations into chunks that stay under Cloud SQL limits

import pandas as pd
import psycopg2
import os
import re
import sqlite3
from io import StringIO
import sys
from dotenv import load_dotenv
from tqdm import tqdm
import time

# --- Configuration ---
CLEANED_CATALOG_PATH = 'cde_catalog_CLEANED.csv'
DEDUPLICATION_REPORT_PATH = "deduplication_report.csv"
WEBLINKS_SQLITE_PATH = 'cdeCatalogs/20250603_2030_cde.sqlite'
ORIGINAL_CATALOG_PATH = 'cdeCatalogs/20250627_cdeCatalog.csv' # For SNOMED, etc.

# OPTIMAL CHUNK SIZES (based on successful 122K record test)
CATALOG_CHUNK_SIZE = 120000     # Single chunk (we know this works)
SYNONYM_CHUNK_SIZE = 200000     # Multiple chunks to stay under limit
EXTERNAL_CHUNK_SIZE = 50000     # Conservative for external codes
WEBLINK_CHUNK_SIZE = 20000      # Conservative for web links

# AGGRESSIVE REST PERIODS
CHUNK_REST_PERIOD = 10          # Rest between chunks of same table
TABLE_REST_PERIOD = 15          # Rest between different tables

def get_fresh_connection():
    """Get a completely fresh database connection"""
    db_host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASSWORD")
    
    return psycopg2.connect(host=db_host, dbname=db_name, user=db_user, password=db_pass)

def log_performance(operation, record_count, duration, data_size_mb=None):
    """Log performance metrics"""
    records_per_sec = record_count / duration if duration > 0 else 0
    print(f"📊 {operation}:")
    print(f"    • Records: {record_count:,}")
    print(f"    • Duration: {duration:.2f}s")
    print(f"    • Rate: {records_per_sec:.1f} records/sec")
    if data_size_mb:
        print(f"    • Data size: {data_size_mb:.2f} MB")
        print(f"    • Throughput: {data_size_mb/duration:.2f} MB/sec")

def migrate_cde_catalog():
    """
    Migrate catalog in single operation (we know this works)
    """
    print("🎯 TABLE 1/4: CDE CATALOG")
    print("--- Migrating ENTIRE CDE Catalog (Proven to Work) ---")
    
    db_columns = [
        "ID", "title", "variable_name", "permissible_values", "unit_of_measure",
        "value_format", "preferred_question_text", "collections", "min_value", "max_value", "version"
    ]
    
    conn = None
    try:
        start_time = time.time()
        
        # Fresh connection for catalog
        conn = get_fresh_connection()
        print("✅ Fresh connection established for catalog")
        
        df = pd.read_csv(CLEANED_CATALOG_PATH, dtype=str)
        print(f"📚 Loaded {len(df):,} rows")

        cols_to_load = [col for col in db_columns if col in df.columns]
        df_final = df[cols_to_load]
        
        table_name = 'public.cde_catalog'
        columns_sql = '", "'.join(cols_to_load)

        print(f"🧹 Clearing existing data...")
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE {table_name} CASCADE;")
            conn.commit()
        
        print(f"🚀 Single operation: {len(df_final):,} records...")
        
        # Create buffer
        buffer = StringIO()
        df_final.to_csv(buffer, index=False, header=False, sep='\t')
        buffer.seek(0)
        
        # Calculate size
        buffer_content = buffer.getvalue()
        data_size_mb = len(buffer_content.encode('utf-8')) / (1024 * 1024)
        buffer.seek(0)
        
        # COPY operation
        copy_start = time.time()
        with conn.cursor() as cur:
            cur.copy_expert(f'COPY {table_name} ("{columns_sql}") FROM STDIN WITH CSV DELIMITER E\'\\t\'', buffer)
            conn.commit()
        copy_duration = time.time() - copy_start
        
        total_duration = time.time() - start_time
        
        print("✅ CATALOG SUCCESS!")
        log_performance("CATALOG", len(df_final), copy_duration, data_size_mb)
        print(f"⏱️ Total time: {total_duration:.2f}s")

    except Exception as e:
        print(f"❌ CATALOG FAILED: {e}")
        raise
    finally:
        if conn:
            conn.close()
            print("🔒 Catalog connection closed")

def migrate_synonyms():
    """
    Migrate synonyms in multiple chunks with fresh connections
    """
    print(f"\n🎯 TABLE 2/4: SYNONYMS")
    print("--- Migrating Synonyms in Multiple Chunks ---")
    
    try:
        start_time = time.time()
        
        # Extract all synonyms first
        synonym_cols = ['ID', 'alternate_titles', 'alternate_headers', 'alternate_terms']
        df = pd.read_csv(CLEANED_CATALOG_PATH, usecols=lambda c: c in synonym_cols, dtype=str)
        df.dropna(subset=synonym_cols[1:], how='all', inplace=True)

        print(f"📚 Processing {len(df):,} CDEs for synonym extraction...")

        all_synonyms = []
        type_map = {'alternate_titles': 'alternate_title', 'alternate_headers': 'alternate_header', 'alternate_terms': 'alternate_term'}

        extraction_start = time.time()
        for _, row in tqdm(df.iterrows(), total=df.shape[0], desc="Extracting Synonyms"):
            cde_id = row['ID']
            for col_name, synonym_type in type_map.items():
                if col_name in row and pd.notna(row[col_name]):
                    synonyms = re.findall(r'(?:"([^"]*)"|([^,{}]+))', str(row[col_name]))
                    for s in synonyms:
                        synonym_clean = s[0] if s[0] else s[1]
                        if synonym_clean:
                            all_synonyms.append((cde_id, synonym_clean.strip(), synonym_type))
        
        extraction_duration = time.time() - extraction_start
        print(f"📊 Extracted {len(all_synonyms):,} synonyms in {extraction_duration:.1f}s")
        
        if not all_synonyms:
            print("📭 No synonyms found")
            return

        # Split into chunks
        total_chunks = (len(all_synonyms) + SYNONYM_CHUNK_SIZE - 1) // SYNONYM_CHUNK_SIZE
        print(f"📦 Splitting into {total_chunks} chunks of {SYNONYM_CHUNK_SIZE:,} records each")
        
        total_inserted = 0
        
        for chunk_idx in range(total_chunks):
            chunk_start_idx = chunk_idx * SYNONYM_CHUNK_SIZE
            chunk_end_idx = min(chunk_start_idx + SYNONYM_CHUNK_SIZE, len(all_synonyms))
            chunk_synonyms = all_synonyms[chunk_start_idx:chunk_end_idx]
            
            print(f"\n🔄 CHUNK {chunk_idx + 1}/{total_chunks}: {len(chunk_synonyms):,} records")
            
            # FRESH CONNECTION for each chunk
            conn = None
            try:
                conn = get_fresh_connection()
                print(f"✅ Fresh connection for chunk {chunk_idx + 1}")
                
                # Convert chunk to DataFrame
                chunk_df = pd.DataFrame(chunk_synonyms, columns=['cde_id', 'synonym_text', 'synonym_type'])
                
                # Create buffer
                buffer = StringIO()
                chunk_df.to_csv(buffer, index=False, header=False, sep='\t')
                buffer.seek(0)
                
                # Calculate size
                buffer_content = buffer.getvalue()
                data_size_mb = len(buffer_content.encode('utf-8')) / (1024 * 1024)
                buffer.seek(0)
                
                # Operation with temp table
                operation_start = time.time()
                with conn.cursor() as cur:
                    # Create temp table
                    cur.execute(f"""
                        CREATE TEMP TABLE temp_synonyms_chunk_{chunk_idx} (
                            cde_id TEXT,
                            synonym_text TEXT,
                            synonym_type TEXT
                        ) ON COMMIT DROP;
                    """)
                    
                    # COPY into temp table
                    cur.copy_expert(
                        f'COPY temp_synonyms_chunk_{chunk_idx} (cde_id, synonym_text, synonym_type) FROM STDIN WITH CSV DELIMITER E\'\\t\'', 
                        buffer
                    )
                    
                    # Insert with conflict resolution
                    cur.execute(f"""
                        INSERT INTO public.cde_synonyms (cde_id, synonym_text, synonym_type)
                        SELECT cde_id, synonym_text, synonym_type 
                        FROM temp_synonyms_chunk_{chunk_idx}
                        ON CONFLICT (cde_id, synonym_text, synonym_type) DO NOTHING;
                    """)
                    
                    chunk_inserted = cur.rowcount
                    conn.commit()
                
                operation_duration = time.time() - operation_start
                total_inserted += chunk_inserted
                
                print(f"✅ Chunk {chunk_idx + 1} SUCCESS!")
                log_performance(f"SYNONYM CHUNK {chunk_idx + 1}", len(chunk_synonyms), operation_duration, data_size_mb)
                print(f"📈 Inserted: {chunk_inserted:,} | Total so far: {total_inserted:,}")
                
            except Exception as e:
                print(f"❌ Chunk {chunk_idx + 1} FAILED: {e}")
                raise
            finally:
                if conn:
                    conn.close()
                    print(f"🔒 Chunk {chunk_idx + 1} connection closed")
            
            # Rest between chunks (except for last chunk)
            if chunk_idx < total_chunks - 1:
                print(f"💤 Resting {CHUNK_REST_PERIOD}s before next chunk...")
                time.sleep(CHUNK_REST_PERIOD)
        
        total_duration = time.time() - start_time
        print(f"\n✅ ALL SYNONYMS COMPLETED!")
        print(f"📊 Total inserted: {total_inserted:,} synonyms")
        print(f"⏱️ Total time: {total_duration:.1f}s ({total_duration/60:.1f} minutes)")

    except Exception as e:
        print(f"❌ SYNONYMS FAILED: {e}")
        raise

def migrate_external_codes():
    """
    Migrate external codes in chunks
    """
    print(f"\n🎯 TABLE 3/4: EXTERNAL CODES")
    print("--- Migrating External Codes ---")
    
    conn = None
    try:
        start_time = time.time()
        
        # Fresh connection for external codes
        conn = get_fresh_connection()
        print("✅ Fresh connection for external codes")
        
        # ICD Code Logic
        print("🔍 Processing ICD codes...")
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
        
        # SNOMED Code Logic
        print("🔍 Processing SNOMED codes...")
        snomed_cols = ['ID', 'snomed_code', 'snomed_alias']
        df_snomed = pd.read_csv(ORIGINAL_CATALOG_PATH, usecols=lambda c: c in snomed_cols, dtype=str)
        df_snomed.dropna(subset=['snomed_code'], inplace=True)
        snomed_records = []
        for _, row in df_snomed.iterrows():
            snomed_records.append((row['ID'], 'SNOMED-CT', row['snomed_code'], row.get('snomed_alias')))
        
        print(f"📊 ICD codes: {len(icd_records):,}")
        print(f"📊 SNOMED codes: {len(snomed_records):,}")
        
        # Insert all external codes
        total_records = len(icd_records) + len(snomed_records)
        if total_records > 0:
            print(f"🚀 Inserting {total_records:,} external codes...")
            
            operation_start = time.time()
            with conn.cursor() as cur:
                # Insert ICD codes
                if len(icd_records) > 0:
                    query = "INSERT INTO public.cde_external_codes (cde_id, code_system, code_value) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING;"
                    cur.executemany(query, icd_records)
                
                # Insert SNOMED codes
                if len(snomed_records) > 0:
                    query = "INSERT INTO public.cde_external_codes (cde_id, code_system, code_value, code_description) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;"
                    cur.executemany(query, snomed_records)
                
                total_inserted = cur.rowcount
                conn.commit()
            
            operation_duration = time.time() - operation_start
            total_duration = time.time() - start_time
            
            print("✅ EXTERNAL CODES SUCCESS!")
            log_performance("EXTERNAL CODES", total_records, operation_duration)
            print(f"📊 Inserted: {total_inserted:,}")
            print(f"⏱️ Total time: {total_duration:.2f}s")
        else:
            print("📭 No external codes found")

    except Exception as e:
        print(f"❌ EXTERNAL CODES FAILED: {e}")
        raise
    finally:
        if conn:
            conn.close()
            print("🔒 External codes connection closed")

def migrate_weblinks():
    """
    Migrate weblinks in single operation
    """
    print(f"\n🎯 TABLE 4/4: WEB LINKS")
    print("--- Migrating Web Links ---")
    
    conn = None
    try:
        start_time = time.time()
        
        # Fresh connection for web links
        conn = get_fresh_connection()
        print("✅ Fresh connection for web links")
        
        with sqlite3.connect(WEBLINKS_SQLITE_PATH) as sqlite_conn:
            links_df = pd.read_sql_query("SELECT ID, web_link FROM cdes WHERE web_link IS NOT NULL", sqlite_conn)
        
        records_to_insert = links_df.to_records(index=False)
        print(f"📊 Found {len(records_to_insert):,} web links")
        
        if len(records_to_insert) == 0: 
            print("📭 No web links found")
            return

        print(f"🚀 Inserting {len(records_to_insert):,} web links...")
        
        operation_start = time.time()
        with conn.cursor() as cur:
            query = "INSERT INTO public.cde_references (cde_id, source_url) VALUES (%s, %s) ON CONFLICT DO NOTHING;"
            cur.executemany(query, records_to_insert)
            inserted_count = cur.rowcount
            conn.commit()
        
        operation_duration = time.time() - operation_start
        total_duration = time.time() - start_time
        
        print("✅ WEB LINKS SUCCESS!")
        log_performance("WEB LINKS", len(records_to_insert), operation_duration)
        print(f"📊 Inserted: {inserted_count:,}")
        print(f"⏱️ Total time: {total_duration:.2f}s")
        
    except Exception as e:
        print(f"❌ WEB LINKS FAILED: {e}")
        raise
    finally:
        if conn:
            conn.close()
            print("🔒 Web links connection closed")

def main():
    """
    FINAL MIGRATION with multi-chunk strategy and fresh connections
    """
    load_dotenv()
    
    db_host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASSWORD")

    if not all([db_host, db_name, db_user, db_pass]):
        print("❌ Missing database connection variables")
        sys.exit(1)

    overall_start = time.time()
    
    try:
        print("🚀 FINAL MIGRATION STRATEGY")
        print("📋 Multi-chunk with fresh connections and aggressive rests")
        print(f"📊 Chunk sizes: Catalog={CATALOG_CHUNK_SIZE:,}, Synonyms={SYNONYM_CHUNK_SIZE:,}")
        print(f"⏸️ Rest periods: Chunks={CHUNK_REST_PERIOD}s, Tables={TABLE_REST_PERIOD}s")
        print("="*70)
        
        # TABLE 1: CDE CATALOG (single operation - proven to work)
        migrate_cde_catalog()
        print(f"💤 Resting {TABLE_REST_PERIOD}s before synonyms...")
        time.sleep(TABLE_REST_PERIOD)
        
        # TABLE 2: SYNONYMS (multiple chunks with fresh connections)
        migrate_synonyms()
        print(f"💤 Resting {TABLE_REST_PERIOD}s before external codes...")
        time.sleep(TABLE_REST_PERIOD)
        
        # TABLE 3: EXTERNAL CODES (single operation)
        migrate_external_codes()
        print(f"💤 Resting {TABLE_REST_PERIOD}s before web links...")
        time.sleep(TABLE_REST_PERIOD)
        
        # TABLE 4: WEB LINKS (single operation)
        migrate_weblinks()
        
        overall_duration = time.time() - overall_start
        print("\n" + "="*70)
        print("🎉 FINAL MIGRATION COMPLETED SUCCESSFULLY!")
        print(f"⏱️ Total time: {overall_duration:.1f}s ({overall_duration/60:.1f} minutes)")
        print("🎯 Strategy: Multi-chunk + fresh connections - SUCCESS!")

    except Exception as e:
        overall_duration = time.time() - overall_start
        print(f"\n💥 MIGRATION FAILED after {overall_duration:.1f}s")
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()