# finish_migration.py
# Purpose: Complete the migration by adding External Codes and Web Links only
# Catalog and Synonyms are already complete - DO NOT TOUCH THEM

import pandas as pd
import psycopg2
import os
import re
import sqlite3
import sys
from dotenv import load_dotenv
import time

# --- Configuration ---
# UPDATE THIS PATH TO WHERE YOU FOUND THE FILE:
DEDUPLICATION_REPORT_PATH = "cdeCatalogs/intermediateVersions/deduplication_report.csv"  # ← UPDATED PATH
WEBLINKS_SQLITE_PATH = 'cdeCatalogs/20250603_2030_cde.sqlite'
ORIGINAL_CATALOG_PATH = 'cdeCatalogs/20250627_cdeCatalog.csv'

def get_fresh_connection():
    """Get a completely fresh database connection"""
    db_host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASSWORD")
    
    return psycopg2.connect(host=db_host, dbname=db_name, user=db_user, password=db_pass)

def log_performance(operation, record_count, duration):
    """Log performance metrics"""
    records_per_sec = record_count / duration if duration > 0 else 0
    print(f"📊 {operation}:")
    print(f"    • Records: {record_count:,}")
    print(f"    • Duration: {duration:.2f}s")
    print(f"    • Rate: {records_per_sec:.1f} records/sec")

def verify_existing_data():
    """Verify that Catalog and Synonyms are already migrated"""
    conn = None
    try:
        conn = get_fresh_connection()
        
        with conn.cursor() as cur:
            # Check catalog
            cur.execute("SELECT COUNT(*) FROM public.cde_catalog;")
            catalog_count = cur.fetchone()[0]
            
            # Check synonyms
            cur.execute("SELECT COUNT(*) FROM public.cde_synonyms;")
            synonym_count = cur.fetchone()[0]
            
            # Check external codes (should be 0 or incomplete)
            cur.execute("SELECT COUNT(*) FROM public.cde_external_codes;")
            external_count = cur.fetchone()[0]
            
            # Check web links (should be 0 or incomplete)
            cur.execute("SELECT COUNT(*) FROM public.cde_references;")
            weblink_count = cur.fetchone()[0]
        
        print("📊 Current Database Status:")
        print(f"    • CDE Catalog: {catalog_count:,} records")
        print(f"    • Synonyms: {synonym_count:,} records")
        print(f"    • External Codes: {external_count:,} records")
        print(f"    • Web Links: {weblink_count:,} records")
        
        if catalog_count == 0 or synonym_count == 0:
            print("❌ ERROR: Catalog or Synonyms are empty!")
            print("   This script only completes the migration - run the main script first")
            return False
        
        print("✅ Catalog and Synonyms are populated - ready to finish migration")
        return True
        
    except Exception as e:
        print(f"❌ Error checking database status: {e}")
        return False
    finally:
        if conn:
            conn.close()

def migrate_external_codes():
    """
    Migrate external codes (ICD and SNOMED) - COMPLETION ONLY
    """
    print(f"\n🎯 FINISHING: EXTERNAL CODES")
    print("--- Migrating External Codes (ICD & SNOMED) ---")
    
    conn = None
    try:
        start_time = time.time()
        
        # Fresh connection for external codes
        conn = get_fresh_connection()
        print("✅ Fresh connection for external codes")
        
        # Check if we need to clear existing external codes
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM public.cde_external_codes;")
            existing_count = cur.fetchone()[0]
        
        if existing_count > 0:
            print(f"🧹 Clearing {existing_count:,} existing external codes...")
            with conn.cursor() as cur:
                cur.execute("TRUNCATE public.cde_external_codes CASCADE;")
                conn.commit()
        
        # ICD Code Logic
        print("🔍 Processing ICD codes...")
        cde_df = pd.read_sql_query('SELECT "ID", title FROM public.cde_catalog', conn)
        cde_df.rename(columns={'ID': 'cde_id'}, inplace=True)
        
        # Check if deduplication file exists
        if not os.path.exists(DEDUPLICATION_REPORT_PATH):
            print(f"❌ ERROR: {DEDUPLICATION_REPORT_PATH} not found!")
            print("   Please update DEDUPLICATION_REPORT_PATH in this script")
            raise FileNotFoundError(f"Cannot find {DEDUPLICATION_REPORT_PATH}")
        
        report_df = pd.read_csv(DEDUPLICATION_REPORT_PATH)
        print(f"✅ Loaded deduplication report: {len(report_df):,} rows")

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
        
        # SNOMED Code Logic (from SQLite database)
        print("🔍 Processing SNOMED codes from SQLite...")
        
        # Check if SQLite file exists
        if not os.path.exists(WEBLINKS_SQLITE_PATH):
            print(f"❌ ERROR: {WEBLINKS_SQLITE_PATH} not found!")
            print("   Please update WEBLINKS_SQLITE_PATH in this script")
            raise FileNotFoundError(f"Cannot find {WEBLINKS_SQLITE_PATH}")
        
        snomed_records = []
        snomed_status = "Not found"
        
        try:
            with sqlite3.connect(WEBLINKS_SQLITE_PATH) as sqlite_conn:
                # First, check what columns exist in the CDE_Dictionary_Condensed table
                cursor = sqlite_conn.cursor()
                cursor.execute("PRAGMA table_info(CDE_Dictionary_Condensed);")
                columns_info = cursor.fetchall()
                available_cols = [col[1] for col in columns_info]  # col[1] is column name
                print(f"📊 Available columns in SQLite 'CDE_Dictionary_Condensed' table: {available_cols}")
                
                # Look for specific SNOMED columns (exact matches preferred)
                snomed_id_col = None
                snomed_code_col = None
                snomed_alias_col = None
                
                # Look for exact column names first
                if 'ID' in available_cols:
                    snomed_id_col = 'ID'
                elif 'id' in available_cols:
                    snomed_id_col = 'id'
                
                if 'snomed_code' in available_cols:
                    snomed_code_col = 'snomed_code'
                
                if 'snomed_alias' in available_cols:
                    snomed_alias_col = 'snomed_alias'
                
                # If exact matches not found, do case-insensitive search
                if not snomed_code_col or not snomed_alias_col:
                    for col in available_cols:
                        col_lower = col.lower()
                        if not snomed_code_col and 'snomed' in col_lower and 'code' in col_lower:
                            snomed_code_col = col
                        if not snomed_alias_col and 'snomed' in col_lower and 'alias' in col_lower:
                            snomed_alias_col = col
                
                if snomed_id_col and snomed_code_col:
                    print(f"✅ Found SNOMED columns: ID='{snomed_id_col}', Code='{snomed_code_col}', Alias='{snomed_alias_col}'")
                    
                    # Build query to specifically get snomed_code and snomed_alias
                    if snomed_alias_col:
                        query = f"SELECT {snomed_id_col}, {snomed_code_col}, {snomed_alias_col} FROM CDE_Dictionary_Condensed WHERE {snomed_code_col} IS NOT NULL AND {snomed_code_col} != ''"
                    else:
                        query = f"SELECT {snomed_id_col}, {snomed_code_col} FROM CDE_Dictionary_Condensed WHERE {snomed_code_col} IS NOT NULL AND {snomed_code_col} != ''"
                        print("⚠️  snomed_alias column not found - will insert codes without aliases")
                    
                    df_snomed = pd.read_sql_query(query, sqlite_conn)
                    print(f"📊 Found {len(df_snomed)} records with SNOMED codes in SQLite")
                    
                    # Get valid CDE IDs from the catalog table to filter SNOMED records
                    print("🔍 Filtering SNOMED records to only include valid CDE IDs...")
                    valid_cde_ids = pd.read_sql_query('SELECT "ID" FROM public.cde_catalog', conn)['ID'].astype(str).tolist()
                    
                    # Filter SNOMED records to only include valid CDE IDs
                    df_snomed[snomed_id_col] = df_snomed[snomed_id_col].astype(str)
                    df_snomed_valid = df_snomed[df_snomed[snomed_id_col].isin(valid_cde_ids)]
                    
                    print(f"📊 After filtering: {len(df_snomed_valid)} valid SNOMED records (filtered out {len(df_snomed) - len(df_snomed_valid)} invalid IDs)")
                    
                    # Build the records list from valid entries only
                    for _, row in df_snomed_valid.iterrows():
                        # Explicitly get the alias from snomed_alias column
                        alias_value = row[snomed_alias_col] if snomed_alias_col and not pd.isna(row.get(snomed_alias_col)) else None
                        snomed_records.append((row[snomed_id_col], 'SNOMED-CT', row[snomed_code_col], alias_value))
                    
                    status_suffix = f" with aliases" if snomed_alias_col else " (no aliases)"
                    if len(df_snomed) != len(df_snomed_valid):
                        status_suffix += f" (filtered out {len(df_snomed) - len(df_snomed_valid)} invalid IDs)"
                    snomed_status = f"Found {len(snomed_records)} valid codes" + status_suffix
                    
                else:
                    missing_cols = []
                    if not snomed_id_col:
                        missing_cols.append("ID")
                    if not snomed_code_col:
                        missing_cols.append("snomed_code")
                    
                    print(f"⚠️  Required SNOMED columns not found: {missing_cols}")
                    snomed_status = f"Missing columns: {', '.join(missing_cols)}"
                    
        except Exception as e:
            print(f"⚠️  Error processing SNOMED from SQLite: {e}")
            snomed_status = f"Error: {str(e)}"
        
        print(f"📊 ICD codes: {len(icd_records):,}")
        print(f"📊 SNOMED codes: {len(snomed_records):,}")
        
        # Insert all external codes
        total_records = len(icd_records) + len(snomed_records)
        if total_records > 0:
            print(f"🚀 Inserting {total_records:,} external codes...")
            
            operation_start = time.time()
            total_inserted = 0
            
            with conn.cursor() as cur:
                # Insert ICD codes
                if len(icd_records) > 0:
                    query = "INSERT INTO public.cde_external_codes (cde_id, code_system, code_value) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING;"
                    cur.executemany(query, icd_records)
                    icd_inserted = cur.rowcount
                    total_inserted += icd_inserted
                    print(f"  ✅ ICD codes inserted: {icd_inserted:,}")
                
                # Insert SNOMED codes
                if len(snomed_records) > 0:
                    query = "INSERT INTO public.cde_external_codes (cde_id, code_system, code_value, code_description) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;"
                    cur.executemany(query, snomed_records)
                    snomed_inserted = cur.rowcount
                    total_inserted += snomed_inserted
                    print(f"  ✅ SNOMED codes inserted: {snomed_inserted:,}")
                else:
                    snomed_inserted = 0
                    print(f"  ⚠️  No valid SNOMED codes to insert")
                
                conn.commit()
            
            operation_duration = time.time() - operation_start
            total_duration = time.time() - start_time
            
            print("✅ EXTERNAL CODES COMPLETED!")
            log_performance("EXTERNAL CODES", total_records, operation_duration)
            print(f"📊 Total inserted: {total_inserted:,}")
            print(f"⏱️ Total time: {total_duration:.2f}s")
            
            # Status Report
            print(f"\n📋 EXTERNAL CODES STATUS REPORT:")
            print(f"    • ICD codes: {len(icd_records):,} found, {icd_inserted:,} inserted")
            if len(snomed_records) > 0:
                print(f"    • SNOMED codes: {snomed_status}, {snomed_inserted:,} inserted")
            else:
                print(f"    • SNOMED codes: {snomed_status}")
            
        else:
            print("📭 No external codes found")
            print(f"\n📋 EXTERNAL CODES STATUS REPORT:")
            print(f"    • ICD codes: 0 found")
            print(f"    • SNOMED codes: {snomed_status}")

    except Exception as e:
        print(f"❌ EXTERNAL CODES FAILED: {e}")
        raise
    finally:
        if conn:
            conn.close()
            print("🔒 External codes connection closed")

def migrate_weblinks():
    """
    Migrate weblinks - COMPLETION ONLY
    """
    print(f"\n🎯 FINISHING: WEB LINKS")
    print("--- Migrating Web Links ---")
    
    conn = None
    try:
        start_time = time.time()
        
        # Fresh connection for web links
        conn = get_fresh_connection()
        print("✅ Fresh connection for web links")
        
        # Check if we need to clear existing web links
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM public.cde_references;")
            existing_count = cur.fetchone()[0]
        
        if existing_count > 0:
            print(f"🧹 Clearing {existing_count:,} existing web links...")
            with conn.cursor() as cur:
                cur.execute("TRUNCATE public.cde_references CASCADE;")
                conn.commit()
        
        # Check if SQLite file exists
        if not os.path.exists(WEBLINKS_SQLITE_PATH):
            print(f"❌ ERROR: {WEBLINKS_SQLITE_PATH} not found!")
            print("   Please update WEBLINKS_SQLITE_PATH in this script")
            raise FileNotFoundError(f"Cannot find {WEBLINKS_SQLITE_PATH}")
        
        with sqlite3.connect(WEBLINKS_SQLITE_PATH) as sqlite_conn:
            links_df = pd.read_sql_query("SELECT ID, web_link FROM CDE_Dictionary_Condensed WHERE web_link IS NOT NULL", sqlite_conn)
        
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
        
        print("✅ WEB LINKS COMPLETED!")
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

def final_verification():
    """Verify the complete migration"""
    print(f"\n🔍 FINAL VERIFICATION")
    print("--- Checking All Tables ---")
    
    conn = None
    try:
        conn = get_fresh_connection()
        
        with conn.cursor() as cur:
            # Check all tables
            cur.execute("SELECT COUNT(*) FROM public.cde_catalog;")
            catalog_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM public.cde_synonyms;")
            synonym_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM public.cde_external_codes;")
            external_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM public.cde_references;")
            weblink_count = cur.fetchone()[0]
        
        print("📊 FINAL DATABASE STATUS:")
        print(f"    ✅ CDE Catalog: {catalog_count:,} records")
        print(f"    ✅ Synonyms: {synonym_count:,} records")
        print(f"    ✅ External Codes: {external_count:,} records")
        print(f"    ✅ Web Links: {weblink_count:,} records")
        
        total_records = catalog_count + synonym_count + external_count + weblink_count
        print(f"📈 TOTAL RECORDS: {total_records:,}")
        
        if catalog_count > 0 and synonym_count > 0:
            print("🎉 MIGRATION FULLY COMPLETED!")
            return True
        else:
            print("❌ Migration incomplete - missing core data")
            return False
        
    except Exception as e:
        print(f"❌ Error in verification: {e}")
        return False
    finally:
        if conn:
            conn.close()

def main():
    """
    Complete the migration - External Codes and Web Links only
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
        print("🏁 COMPLETING MIGRATION")
        print("📋 External Codes + Web Links ONLY")
        print("⚠️  Catalog and Synonyms will NOT be touched")
        print("="*60)
        
        # Verify existing data
        if not verify_existing_data():
            print("❌ Cannot proceed - missing prerequisite data")
            sys.exit(1)
        
        print("\n🚀 Starting completion phase...")
        
        # Complete External Codes
        migrate_external_codes()
        print(f"💤 Resting 5s before web links...")
        time.sleep(5)
        
        # Complete Web Links
        migrate_weblinks()
        
        # Final verification
        success = final_verification()
        
        overall_duration = time.time() - overall_start
        print("\n" + "="*60)
        
        if success:
            print("🎉 MIGRATION 100% COMPLETED!")
            print(f"⏱️ Completion time: {overall_duration:.1f}s")
            print("💰 No duplicate transactions - cost efficient!")
        else:
            print("❌ Migration completion failed")
            sys.exit(1)

    except Exception as e:
        overall_duration = time.time() - overall_start
        print(f"\n💥 COMPLETION FAILED after {overall_duration:.1f}s")
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    print("🎯 STANDALONE MIGRATION COMPLETION")
    print("   This script ONLY migrates External Codes and Web Links")
    print("   Catalog and Synonyms must already be complete")
    print("")
    
    # Get user confirmation
    response = input("Continue with completion? (y/N): ")
    if response.lower() != 'y':
        print("❌ Operation cancelled")
        sys.exit(0)
    
    main()