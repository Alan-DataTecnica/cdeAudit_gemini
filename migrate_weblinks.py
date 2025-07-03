#!/usr/bin/env python3
"""
Quick fix: Migrate ONLY web links with numpy datatype fix
"""
import pandas as pd
import psycopg2
import sqlite3
import sys
from dotenv import load_dotenv
import time
import os
 
# Configuration
WEBLINKS_SQLITE_PATH = 'cdeCatalogs/20250603_2030_cde.sqlite'

def get_fresh_connection():
    """Get a fresh database connection"""
    load_dotenv()
    
    db_host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASSWORD")
    
    return psycopg2.connect(host=db_host, dbname=db_name, user=db_user, password=db_pass)

def migrate_weblinks_fixed():
    """Migrate weblinks with numpy datatype fix"""
    print("🎯 WEB LINKS MIGRATION (DATATYPE FIX)")
    print("--- Migrating Web Links Only ---")
    
    conn = None
    try:
        start_time = time.time()
        
        # Fresh connection
        conn = get_fresh_connection()
        print("✅ Fresh connection established")
        
        # Check current web links count
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM public.cde_references;")
            existing_count = cur.fetchone()[0]
        
        if existing_count > 0:
            print(f"🧹 Clearing {existing_count:,} existing web links...")
            with conn.cursor() as cur:
                cur.execute("TRUNCATE public.cde_references CASCADE;")
                conn.commit()
        
        # Get web links from SQLite
        print("📊 Reading web links from SQLite...")
        with sqlite3.connect(WEBLINKS_SQLITE_PATH) as sqlite_conn:
            links_df = pd.read_sql_query("SELECT ID, web_link FROM CDE_Dictionary_Condensed WHERE web_link IS NOT NULL", sqlite_conn)
        
        print(f"📊 Found {len(links_df):,} web links in SQLite")
        
        if len(links_df) == 0:
            print("📭 No web links found")
            return
        
        # Get valid CDE IDs from catalog to filter web links
        print("🔍 Filtering web links to only include valid CDE IDs...")
        with conn.cursor() as cur:
            cur.execute('SELECT "ID" FROM public.cde_catalog')
            valid_cde_ids = {str(row[0]) for row in cur.fetchall()}  # Convert to set of strings
        
        # FIX: Convert numpy types to Python native types AND filter valid IDs
        print("🔧 Converting data types and filtering valid IDs...")
        records_to_insert = []
        filtered_count = 0
        
        for _, row in links_df.iterrows():
            # Convert numpy.int64 to Python int, then string
            cde_id = str(int(row['ID']))
            
            # Only include if CDE ID exists in catalog
            if cde_id in valid_cde_ids:
                web_link = str(row['web_link']) 
                records_to_insert.append((cde_id, web_link))
            else:
                filtered_count += 1
        
        print(f"📊 After filtering: {len(records_to_insert):,} valid web links (filtered out {filtered_count:,} invalid IDs)")
        
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
        print(f"📊 Performance:")
        print(f"    • Records: {len(records_to_insert):,}")
        print(f"    • Duration: {operation_duration:.2f}s")
        print(f"    • Rate: {len(records_to_insert)/operation_duration:.1f} records/sec")
        print(f"📊 Inserted: {inserted_count:,} web links")
        if filtered_count > 0:
            print(f"⚠️  Filtered out: {filtered_count:,} web links with invalid CDE IDs")
        print(f"⏱️ Total time: {total_duration:.2f}s")
        
        return True
        
    except Exception as e:
        print(f"❌ WEB LINKS FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if conn:
            conn.close()
            print("🔒 Web links connection closed")

def verify_completion():
    """Verify all tables are complete"""
    print(f"\n🔍 FINAL VERIFICATION")
    
    conn = None
    try:
        conn = get_fresh_connection()
        
        with conn.cursor() as cur:
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
        
        if all([catalog_count > 0, synonym_count > 0, external_count > 0, weblink_count > 0]):
            print("🎉 COMPLETE MIGRATION SUCCESSFUL!")
            return True
        else:
            print("⚠️  Some tables still empty")
            return False
        
    except Exception as e:
        print(f"❌ Verification error: {e}")
        return False
    finally:
        if conn:
            conn.close()

def main():
    print("🔧 WEB LINKS QUICK FIX")
    print("📋 Fixes numpy.int64 datatype issue")
    print("⚠️  This will ONLY migrate web links")
    print()
    
    response = input("Migrate web links with datatype fix? (y/N): ")
    if response.lower() != 'y':
        print("❌ Operation cancelled")
        return
    
    print("="*50)
    
    success = migrate_weblinks_fixed()
    
    if success:
        verify_completion()
        print("\n🎉 WEB LINKS MIGRATION COMPLETED!")
        print("💰 Quick fix - minimal time and cost")
    else:
        print("\n💥 Web links migration failed")
        sys.exit(1)

if __name__ == "__main__":
    main()