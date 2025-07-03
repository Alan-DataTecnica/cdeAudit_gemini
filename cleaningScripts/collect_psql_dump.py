#!/usr/bin/env python3
"""
Create a complete PostgreSQL backup from Cloud SQL via proxy
"""
import os
import subprocess
import time
from datetime import datetime
from dotenv import load_dotenv

def create_postgres_backup():
    """Create a complete PostgreSQL dump"""
    load_dotenv()
    
    # Database connection info
    db_host = os.getenv("DB_HOST", "localhost")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASSWORD")
    
    if not all([db_name, db_user, db_pass]):
        print("❌ Missing database connection variables")
        return False
    
    # Create timestamp for filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = f"cde_database_backup_{timestamp}.sql"
    
    print("🗄️ PostgreSQL Database Backup")
    print("="*50)
    print(f"📂 Host: {db_host}")
    print(f"📂 Database: {db_name}")
    print(f"👤 User: {db_user}")
    print(f"📄 Output file: {backup_filename}")
    print()
    
    # Set password environment variable for pg_dump
    env = os.environ.copy()
    env['PGPASSWORD'] = db_pass
    
    # Build pg_dump command
    cmd = [
        'pg_dump',
        '-h', db_host,
        '-U', db_user,
        '-d', db_name,
        '--verbose',
        '--clean',                    # Add DROP statements
        '--create',                   # Add CREATE DATABASE statement
        '--if-exists',               # Use IF EXISTS for drops
        '--quote-all-identifiers',   # Quote all identifiers
        '--no-owner',                # Don't set ownership
        '--no-privileges',           # Don't dump privileges
        '--format=plain',            # Plain SQL format
        '--file', backup_filename
    ]
    
    print("🚀 Starting backup...")
    print(f"📋 Command: {' '.join(cmd[:6])} [options] > {backup_filename}")
    
    try:
        start_time = time.time()
        
        # Run pg_dump
        result = subprocess.run(cmd, env=env, capture_output=True, text=True, check=True)
        
        duration = time.time() - start_time
        
        # Check if file was created and get size
        if os.path.exists(backup_filename):
            file_size = os.path.getsize(backup_filename)
            file_size_mb = file_size / (1024 * 1024)
            
            print("✅ BACKUP SUCCESSFUL!")
            print(f"📊 Backup Statistics:")
            print(f"    • File: {backup_filename}")
            print(f"    • Size: {file_size_mb:.2f} MB ({file_size:,} bytes)")
            print(f"    • Duration: {duration:.1f} seconds")
            
            # Quick validation - check file contents
            with open(backup_filename, 'r') as f:
                first_lines = f.read(1000)
                if 'PostgreSQL database dump' in first_lines:
                    print(f"✅ File validation: Valid PostgreSQL dump")
                else:
                    print(f"⚠️  File validation: Format unclear")
            
            print(f"\n💾 Backup saved as: {os.path.abspath(backup_filename)}")
            return True
            
        else:
            print("❌ Backup file was not created")
            return False
            
    except subprocess.CalledProcessError as e:
        print(f"❌ pg_dump failed with exit code {e.returncode}")
        print(f"❌ Error: {e.stderr}")
        return False
    except FileNotFoundError:
        print("❌ pg_dump command not found")
        print("💡 Install PostgreSQL client tools:")
        print("   sudo apt install postgresql-client")
        return False
    except Exception as e:
        print(f"❌ Backup failed: {e}")
        return False

def verify_backup_adequacy():
    """Explain what the backup contains and how to restore"""
    print("\n" + "="*60)
    print("📋 BACKUP ADEQUACY VERIFICATION")
    print("="*60)
    
    print("\n✅ This backup contains:")
    print("   • Complete database schema (CREATE statements)")
    print("   • All table data (INSERT statements)")
    print("   • Indexes and constraints")
    print("   • Sequences and their current values")
    print("   • DROP statements (with --clean flag)")
    print("   • CREATE DATABASE statement (with --create flag)")
    
    print("\n🔄 To restore this backup:")
    print("   1. Create new Cloud SQL instance (or use existing)")
    print("   2. Connect via proxy: ./cloud-sql-proxy [connection-string]")
    print("   3. Restore: psql -h localhost -U postgres < backup_file.sql")
    print("   4. Or restore to new DB: psql -h localhost -U postgres -f backup_file.sql")
    
    print("\n💰 Cost savings:")
    print("   • Backup to local file = FREE")
    print("   • No Cloud SQL storage costs for backup")
    print("   • Can restore to any PostgreSQL instance")
    
    print("\n🛡️ What this backup gives you:")
    print("   • Complete disaster recovery")
    print("   • Ability to restore to any point in time (when backup was taken)")
    print("   • Migration to different cloud providers")
    print("   • Local development database setup")
    print("   • Data analysis on local machine")

def main():
    print("🗄️ PostgreSQL Cloud SQL Backup Tool")
    print()
    
    # Check if Cloud SQL proxy is running
    try:
        result = subprocess.run(['nc', '-z', 'localhost', '5432'], 
                              capture_output=True, check=True)
        print("✅ Cloud SQL proxy detected on localhost:5432")
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("⚠️  Cloud SQL proxy not detected on localhost:5432")
        print("   Make sure your proxy is running first!")
        response = input("Continue anyway? (y/N): ")
        if response.lower() != 'y':
            return
    
    print()
    response = input("Create PostgreSQL backup? (y/N): ")
    if response.lower() != 'y':
        print("❌ Backup cancelled")
        return
    
    success = create_postgres_backup()
    
    if success:
        verify_backup_adequacy()
        print("\n🎉 Backup process completed successfully!")
    else:
        print("\n💥 Backup process failed")

if __name__ == "__main__":
    main()