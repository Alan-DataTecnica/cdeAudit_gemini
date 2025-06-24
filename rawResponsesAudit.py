# run_audit.py

import os
import json
import logging
import pandas as pd
from pydantic import BaseModel, RootModel, ValidationError, Field
from typing import List, Optional
from tqdm import tqdm
from json_repair import repair_json

# --- 1. CONFIGURATION ---

# --- Set the target directory containing your raw JSON responses ---
# --- You can change this path to point to your actual directory ---
RAW_RESPONSES_DIR = 'outputs/stage_3/raw_responses'

# --- Define the output report file ---
AUDIT_REPORT_PATH = 'outputs/responseAudits/audit_report.csv'

# --- Basic Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 2. PYDANTIC MODELS FOR VALIDATION ---
# This flexible model validates the expected structure of the JSON responses.

class Suggestion(BaseModel):
    """Defines the structure of the 'suggestions' object for a single CDE."""
    title: Optional[str] = None
    collections: Optional[str] = None
    permissible_values: Optional[str] = None
    quality_review_flag: Optional[bool] = Field(None, alias='quality_review_flag')
    quality_score: Optional[int] = Field(None, alias='quality_score')

class CDEUpdate(BaseModel):
    """Defines the structure of a single item in the root JSON array."""
    ID: str
    suggestions: Suggestion

# --- FIX: Updated for Pydantic V2 ---
# Inherit from RootModel to validate a list as the top-level object.
class ResponseFile(RootModel):
    """The overall structure of a valid raw response file is a list of CDEUpdate objects."""
    root: List[CDEUpdate]


# --- 3. AUDIT LOGIC ---

def run_json_audit(directory_path: str):
    """
    Analyzes all JSON files in a directory, attempts repairs, validates structure,
    and generates a detailed audit report.
    """
    if not os.path.isdir(directory_path):
        logging.error(f"Directory not found: '{directory_path}'. Please check the RAW_RESPONSES_DIR path.")
        return

    all_files = [f for f in os.listdir(directory_path) if f.endswith(('.json', '.txt'))]
    logging.info(f"Starting audit of {len(all_files)} files in '{directory_path}'...")

    audit_results = []
    
    # Statistics counters
    stats = {
        'total_files': len(all_files),
        'valid_and_parsed': 0,
        'repaired_and_parsed': 0,
        'empty_files': 0,
        'irreparably_malformed': 0,
        'pydantic_validation_failures': 0,
        'total_suggestions': 0
    }

    for filename in tqdm(all_files, desc="Auditing Files"):
        file_path = os.path.join(directory_path, filename)
        status = ''
        notes = ''
        suggestion_count = 0

        # Check for empty files first
        if os.path.getsize(file_path) == 0:
            status = 'Empty'
            stats['empty_files'] += 1
            audit_results.append({'filename': filename, 'status': status, 'suggestion_count': 0, 'notes': 'File is 0 bytes.'})
            continue

        with open(file_path, 'r', encoding='utf-8') as f:
            raw_content = f.read()

        # Check for functionally empty content
        if not raw_content.strip():
            status = 'Empty'
            stats['empty_files'] += 1
            audit_results.append({'filename': filename, 'status': status, 'suggestion_count': 0, 'notes': 'File contains only whitespace.'})
            continue

        parsed_data = None
        # 1. First attempt: Standard JSON parsing
        try:
            parsed_data = json.loads(raw_content)
            status = 'Valid'
        except json.JSONDecodeError:
            # 2. Second attempt: Repair and parse
            try:
                repaired_content = repair_json(raw_content)
                parsed_data = json.loads(repaired_content)
                status = 'Repaired'
                notes = 'Original was malformed; successfully repaired.'
            except (json.JSONDecodeError, ValueError) as e:
                status = 'Malformed'
                notes = f"Failed standard parsing and could not be repaired. Error: {e}"
                stats['irreparably_malformed'] += 1

        # 3. Third step: Pydantic validation if parsing was successful
        if parsed_data is not None:
            try:
                # --- FIX: Updated for Pydantic V2 ---
                # Use model_validate() instead of parse_obj()
                validated_file = ResponseFile.model_validate(parsed_data)
                
                # --- FIX: Updated for Pydantic V2 ---
                # Access the data via the .root attribute
                suggestion_count = len(validated_file.root)
                stats['total_suggestions'] += suggestion_count
                
                if status == 'Valid':
                    stats['valid_and_parsed'] += 1
                else: # Repaired
                    stats['repaired_and_parsed'] += 1
            except ValidationError as e:
                status = 'Structurally Invalid'
                notes = f"Parsed as JSON but failed Pydantic validation. Error: {e}"
                stats['pydantic_validation_failures'] += 1
        
        audit_results.append({
            'filename': filename,
            'status': status,
            'suggestion_count': suggestion_count,
            'notes': notes
        })

    # --- 4. GENERATE REPORT ---
    
    # Save detailed CSV report
    report_df = pd.DataFrame(audit_results)
    report_df.to_csv(AUDIT_REPORT_PATH, index=False)
    logging.info(f"\nDetailed audit report saved to: {AUDIT_REPORT_PATH}")

    # Print summary statistics
    print("\n--- JSON Audit Summary ---")
    print(f"Total Files Processed: {stats['total_files']}")
    print("-" * 26)
    print(f"✔️ Valid & Parsed: {stats['valid_and_parsed']}")
    print(f"🔧 Repaired & Parsed: {stats['repaired_and_parsed']}")
    print(f"📝 Structurally Invalid (Failed Pydantic): {stats['pydantic_validation_failures']}")
    print(f"❌ Irreparably Malformed: {stats['irreparably_malformed']}")
    print(f"텅 Empty Files: {stats['empty_files']}")
    print("-" * 26)
    print(f"📈 Total Suggestions Found: {stats['total_suggestions']}")
    print("--------------------------\n")

# --- 5. MAIN EXECUTION ---
if __name__ == "__main__":
    run_json_audit(RAW_RESPONSES_DIR)