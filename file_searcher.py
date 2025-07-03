#!/usr/bin/env python3
"""
Search for deduplication_report.csv in the current workspace
"""
import os
import glob

def search_for_file():
    target_file = "deduplication_report.csv"
    
    print(f"🔍 Searching for '{target_file}' in current workspace...")
    print(f"📂 Starting from: {os.getcwd()}")
    print("=" * 60)
    
    found_files = []
    
    # Method 1: Walk through all directories
    print("📁 Method 1: Walking through all directories...")
    for root, dirs, files in os.walk("."):
        for file in files:
            if file.lower() == target_file.lower():
                full_path = os.path.join(root, file)
                found_files.append(full_path)
                print(f"✅ FOUND: {full_path}")
    
    # Method 2: Glob pattern search
    print(f"\n📁 Method 2: Glob pattern search...")
    patterns = [
        f"**/{target_file}",
        f"**/deduplication*.csv",
        f"**/*dedup*.csv",
        f"**/*duplicate*.csv"
    ]
    
    for pattern in patterns:
        matches = glob.glob(pattern, recursive=True)
        for match in matches:
            if match not in found_files:
                found_files.append(match)
                print(f"📄 PATTERN MATCH: {match}")
    
    # Method 3: Look for similar files
    print(f"\n📁 Method 3: Looking for similar CSV files...")
    csv_files = glob.glob("**/*.csv", recursive=True)
    similar_files = []
    
    keywords = ['dedup', 'duplicate', 'report']
    for csv_file in csv_files:
        filename = os.path.basename(csv_file).lower()
        if any(keyword in filename for keyword in keywords):
            similar_files.append(csv_file)
    
    if similar_files:
        print("📊 Similar CSV files found:")
        for file in similar_files:
            print(f"   • {file}")
    
    # Summary
    print("\n" + "=" * 60)
    if found_files:
        print(f"🎉 SUCCESS! Found {len(found_files)} matching file(s):")
        for file in found_files:
            print(f"   ✅ {os.path.abspath(file)}")
    else:
        print(f"❌ '{target_file}' not found in current workspace")
        print("\n💡 Suggestions:")
        print("   1. Check if the file was moved or renamed")
        print("   2. Search in parent directories")
        print("   3. Check if it's in a different project folder")
        
        if similar_files:
            print(f"\n🔍 But found these similar files:")
            for file in similar_files:
                print(f"   📄 {file}")
    
    return found_files

if __name__ == "__main__":
    found_files = search_for_file()
    
    # Also search in common parent directories
    if not found_files:
        print(f"\n🔍 Expanding search to parent directories...")
        
        current = os.getcwd()
        parent = os.path.dirname(current)
        grandparent = os.path.dirname(parent)
        
        search_dirs = [parent, grandparent]
        
        for search_dir in search_dirs:
            if os.path.exists(search_dir):
                print(f"📂 Searching in: {search_dir}")
                for root, dirs, files in os.walk(search_dir):
                    for file in files:
                        if file.lower() == "deduplication_report.csv":
                            full_path = os.path.join(root, file)
                            print(f"✅ FOUND IN PARENT: {full_path}")
                            found_files.append(full_path)
    
    if found_files:
        print(f"\n📋 Use one of these paths in your script:")
        for file in found_files:
            print(f"   DEDUPLICATION_REPORT_PATH = '{file}'")