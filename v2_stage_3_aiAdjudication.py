import os
import json
import time
import logging
import sys
import pandas as pd
import datetime
import concurrent.futures
import requests # Added for REST API calls
from typing import List, Dict, Any, Optional, Literal

# Pydantic for validation
from pydantic import BaseModel, ValidationError, RootModel

# TQDM for progress bars
from tqdm import tqdm

# --- 1. CONFIGURATION ---

# -- File Paths --
INPUT_COMMUNITIES_PATH = os.path.join('outputs', 'stage_2', 'community_definitions.json')
PROCESSED_CATALOG_PATH = os.path.join('outputs', 'stage_1', 'cde_catalog_processed.csv')
OUTPUT_DIR = "stage3_adjudication_output"
RAW_DIR = os.path.join(OUTPUT_DIR, "raw_responses")
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(RAW_DIR, exist_ok=True)

# -- Log and Manifest Files --
MANIFEST_FILE = os.path.join(OUTPUT_DIR, "manifest.json")
ERROR_LOG_PATH = os.path.join(OUTPUT_DIR, "error_log.txt")
TOKEN_LOG_PATH = os.path.join(OUTPUT_DIR, "token_log.csv")
LOG_FILE = os.path.join(OUTPUT_DIR, "pipeline.log")

# -- Worker and Safety Controls --
MAX_WORKERS = 8
MAX_CONSECUTIVE_ERRORS = 10
COST_LIMIT_USD = 250.0

# -- API and Model Configuration --
MODEL_NAME = "models/gemini-2.5-pro"
BASE_API_URL = "https://generativelanguage.googleapis.com/v1beta"
CACHE_DISPLAY_NAME = "cde_adjudication_cache"
TOKEN_PRICING = {
    "input": 0.00000125,
    "output": 0.00001000,
    "cached": 0.00000031
}

# --- 2. THE SYSTEM PROMPT ---
# The comprehensive prompt we designed for Pass 1
SYSTEM_PROMPT = """
### ROLE ###
You are a meticulous data architect and clinical data manager specializing in CDE (Common Data Element) harmonization for biomedical research.

### PRIMARY MISSION ###
Your mission is to perform a first-pass analysis on each CDE within the provided "CDE Group for Review". You will return a single, valid JSON array `[...]`, where each object corresponds to one CDE from the input group. Each object MUST contain ONLY two keys: "ID" and "suggestions". Your entire response MUST be only the JSON array, with no additional text or explanations.

### CORE TASKS ###
For each CDE, generate a "suggestions" object containing fields for any improvements you identify. If a CDE is already perfect, return an empty "suggestions" object for it.

1.  **Core Field Enrichment:** Review and provide improved suggestions for the following fields if they are missing or low-quality: `title`, `short_description`, `synonymous_terms`, `alternate_titles`, and `alternate_headers`.
    - `short_description` should be a direct clinical definition, beginning with the concept itself (e.g., "The number of heart contractions..." instead of "This CDE measures the number of heart contractions...").

2.  **`variable_name` Correction (Conditional Task):** If the input CDE includes `"is_bad_variable_name": true`, you MUST suggest a corrected `variable_name`. The new name must be `snake_case`, start with a letter, and be **20 characters or less**. Use the standard abbreviations from the reference list.

3.  **`collections` Assignment:** Select up to five of the most relevant categories from the `CDE COLLECTIONS` reference list. The output must be a single, pipe-separated string.

4.  **Standard Code Suggestion:** Analyze the CDE's purpose to determine the most appropriate terminology systems. Suggest one or more relevant codes from `ICD-10-CM` (diagnoses), `LOINC` (tests/measurements), or `SNOMED CT` (other findings). The output must be a single, pipe-separated string in a field named `suggested_codes`. The string MUST contain only the alphanumeric code itself (e.g., "8480-6" or "E11.9"). DO NOT include the system name (like "LOINC:") in the string.

5.  **Quality & Redundancy Analysis:**
    - `quality_score`: Provide an integer rating from 1 (minor issues) to 5 (critical issues) based on the overall quality of the CDE's metadata.
    - `redundancy_flag`: Set to `true` if the CDE is clearly a duplicate or redundant with another CDE in the `Community Context`.
    - `redundant_with_ids`: If `redundancy_flag` is true, provide a pipe-separated string of the CDE IDs it is redundant with.

6.  **Value Definition Flagging (Critical Task):**
    - Your task is ONLY to flag whether the CDE needs advanced value review. DO NOT suggest any `value_mapping` or `permissible_values` in this pass.
    - Analyze the CDE's value-related fields and generate a boolean flag, `"requires_advanced_value_review"`.
    - Set this flag to `true` if the value definition is complex, ambiguous, represents a standard instrument (like MMSE, GDS, MoCA), or could be improved with multiple variants.
    - Set this flag to `false` only if the value definition is simple, complete, and unambiguous (e.g., a standard date, a simple binary Yes/No).

### REFERENCE LIST 1: CDE COLLECTIONS ###
'Availability', 'Family History', 'Screening', 'Pathology', 'NACC', 'ADC', 'ADNI', 'PPMI', 'Neuroimaging', 'Genomics', 'Digital', 'Cognitive', 'Clinical', 'Biomarkers', 'Demographics', 'Vital Signs', 'Laboratory Results', 'Medication Administration', 'Patient-Reported Outcomes', 'Clinician-Reported Outcomes', 'Medical History & Events', 'Physical Examination', 'Diagnosis', 'Oncology', 'Cardiology', 'Neurology', 'Endocrinology', 'Infectious Disease', 'Genomic Markers', 'Imaging', 'Study Protocol & Administration', 'Genetics', 'Experimental Model', 'Transcriptomics', 'Epigenomics', 'Metabolomics', 'Proteomics', 'Behavioral', 'Social', 'Environmental', 'Epidemiological', 'Ethnicity', 'Molecular', 'Cardiovascular', 'Metabolic Disorders', 'Neurodegenerative Diseases', 'Geriatric', 'Rheumatology'

### REFERENCE LIST 2: ABBREVIATIONS FOR `variable_name` ###
- **General:** Num, No, Amt, Avg, Tot, Cnt, Msr, Idx, Scr, Lvl, Val, Pct, Rt, Freq, Vol, Sz, Wt, Qst, Resp, Summ, Desc, ID, Cat, Typ, Stat, Chg, Diff.
- **Medical:** Dx, Trt, Tx, Asmt, Hx, Med, Clin, Sympt, Proc, Exam.
- **Time:** Ag, Dt, Yr, Mo, Dy, Dly, Wkly, Dur, Prd, Ons.

### OUTPUT SCHEMA ###
Each object in the output array must contain two keys: "ID" (string) and "suggestions" (object).
**CRITICAL:** The "suggestions" object must be a flat key-value structure. Do not use nested JSON objects for any suggestion in this pass.

### EXAMPLE ###
**INPUT CDE:**
{
  "ID": "1501",
  "title": "GDS - Satisfied with life",
  "short_description": "",
  "permissible_values": "0=Yes, 1=No",
  "quality_flags": { "is_bad_variable_name": false }
}

**YOUR RESPONSE (JSON OBJECT FOR THIS CDE):**
{
  "ID": "1501",
  "suggestions": {
    "title": "Geriatric Depression Scale (GDS): Satisfaction with life",
    "short_description": "A component of the 15-item Geriatric Depression Scale assessing the respondent's general satisfaction with their life.",
    "synonymous_terms": "GDS Q1 - Satisfied with life|GDS Item - Life Satisfaction",
    "collections": "Patient-Reported Outcomes|Geriatric|Cognitive",
    "suggested_codes": "445123001|LP17321-3",
    "quality_score": 2,
    "requires_advanced_value_review": true
  }
}
"""
# --- 3. Pydantic Models ---
class SubGroup(BaseModel):
    group_id: str
    member_cde_ids: List[int]

class ParentCommunity(BaseModel):
    community_id: str
    total_cde_count: int
    member_cde_ids: List[int]
    sub_groups: List[SubGroup]

class Suggestions(BaseModel):
    title: Optional[str] = None
    short_description: Optional[str] = None
    synonymous_terms: Optional[str] = None
    alternate_titles: Optional[str] = None
    alternate_headers: Optional[str] = None
    variable_name: Optional[str] = None
    collections: Optional[str] = None
    suggested_codes: Optional[str] = None
    quality_score: Optional[int] = None
    redundancy_flag: Optional[bool] = None
    redundant_with_ids: Optional[str] = None
    requires_advanced_value_review: Optional[bool] = None

class AdjudicationResult(BaseModel):
    ID: str
    suggestions: Suggestions

class AIResponse(RootModel[List[AdjudicationResult]]):
    pass

# --- 4. Helper and API Functions ---

def log_error(group_id: str, err: Any, details: Dict[str, Any] = None):
    error_message = f"--- ERROR: group {group_id} @ {time.ctime()} ---\n"
    error_message += f"Error Type: {type(err).__name__}\n"
    error_message += f"Error Message: {str(err)}\n"
    if details:
        error_message += "--- Details ---\n"
        for key, value in details.items():
            error_message += f"{key}: {value}\n"
        error_message += "---------------\n"
    with open(ERROR_LOG_PATH, 'a') as f: f.write(error_message + "\n\n")
    logging.error(f"Logged critical error for group {group_id}. See {ERROR_LOG_PATH}.")

def log_token_usage(group_id: str, usage_metadata: Dict[str, int], call_cost: float):
    header = "group_id,prompt_tokens,cached_tokens,output_tokens,total_tokens,call_cost_usd\n"
    if not os.path.exists(TOKEN_LOG_PATH):
        with open(TOKEN_LOG_PATH, "w") as f: f.write(header)
    with open(TOKEN_LOG_PATH, "a") as f:
        prompt_tokens = usage_metadata.get('promptTokenCount', 0)
        cached_tokens = usage_metadata.get('cachedContentTokenCount', 0)
        output_tokens = usage_metadata.get('candidatesTokenCount', 0)
        total_tokens = usage_metadata.get('totalTokenCount', 0)
        f.write(f"{group_id},{prompt_tokens},{cached_tokens},{output_tokens},{total_tokens},{call_cost:.8f}\n")

def load_and_validate_communities(filepath: str, cde_lookup: dict) -> List[ParentCommunity]:
    logging.info(f"Attempting to load and validate community file from: {filepath}")
    if not os.path.exists(filepath):
        logging.error(f"Input file not found: {filepath}"); return []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
            validated_data: List[ParentCommunity] = [ParentCommunity.parse_obj(item) for item in data]
        if not validated_data:
            logging.warning("Validation successful, but community file is empty."); return []
        total_groups = sum(len(c.sub_groups) for c in validated_data)
        community_sizes = [c.total_cde_count for c in validated_data]
        logging.info("--- Community File Validation SUCCESS ---")
        logging.info(f"Total Parent Communities: {len(validated_data):,}")
        logging.info(f"Total Subdivided Groups: {total_groups:,}")
        if community_sizes:
            logging.info(f"Community Size (min/avg/max): {min(community_sizes)} / {pd.Series(community_sizes).mean():.1f} / {max(community_sizes)}")
        
        first_community = validated_data[0]
        first_group = first_community.sub_groups[0]
        first_group_ids = [str(id) for id in first_group.member_cde_ids]
        sample_cde_data = [cde_lookup.get(id, {}) for id in first_group_ids]
        print("\n" + "="*80)
        print("--- API CALL SAMPLE & APPROVAL ---")
        print(f"The first API call will be for Group ID: '{first_group.group_id}' with {len(sample_cde_data)} CDEs.")
        print("Sample CDE data to be sent for review (first 2 CDEs):")
        print(json.dumps(sample_cde_data[:2], indent=2))
        print("="*80)
        approval = input("Proceed with processing all groups? (Y/N): ").strip().upper()
        if approval != 'Y':
            logging.warning("Processing aborted by user."); return []
        return validated_data
    except (ValidationError, json.JSONDecodeError, TypeError, IndexError, KeyError) as e:
        logging.error(f"Failed to load, validate, or generate sample from community file: {e}"); return []

def load_cde_catalog(filepath: str) -> Dict[str, Dict[str, Any]]:
    try:
        logging.info(f"Loading processed CDE catalog from: {filepath}")
        df = pd.read_csv(filepath, dtype={'ID': str}, low_memory=False)
        df.set_index('ID', inplace=True)
        return df.where(pd.notna(df), None).to_dict('index')
    except FileNotFoundError:
        logging.error(f"Processed CDE catalog not found at: {filepath}"); return {}

def load_manifest(file_path: str) -> Dict[str, str]:
    if not os.path.exists(file_path): return {}
    with open(file_path, "r") as f: return json.load(f)

def save_manifest(file_path: str, manifest: Dict[str, str]):
    with open(file_path, "w") as f: json.dump(manifest, f, indent=2)

def create_cache_via_rest(api_key: str, system_prompt: str, community_context: str) -> Optional[str]:
    url = f"{BASE_API_URL}/cachedContents?key={api_key}"
    body = {
        "model": MODEL_NAME,
        "displayName": CACHE_DISPLAY_NAME,
        "system_instruction": {"parts": [{"text": system_prompt}]},
        "contents": [{"role": "user", "parts": [{"text": "### COMMUNITY CONTEXT ###\n" + community_context}]}],
        "ttl": "3600s"
    }
    logging.info(f"Attempting to create cache via REST...")
    try:
        resp = requests.post(url, headers={"Content-Type": "application/json"}, json=body, timeout=60)
        resp.raise_for_status()
        name = resp.json().get("name")
        logging.info(f"Cache created successfully: {name}")
        return name
    except requests.exceptions.RequestException as e:
        log_error("CACHE_CREATE", e, {"response_body": e.response.text if e.response else "N/A"})
        return None

def delete_cache(cache_name: str, api_key: str):
    if not cache_name: return
    logging.info(f"Deleting cache: {cache_name}...")
    url = f"{BASE_API_URL}/{cache_name}?key={api_key}"
    try: requests.delete(url, timeout=15)
    except requests.exceptions.RequestException as e: log_error(f"CACHE_DELETE_{cache_name}", e)

def generate_content_via_rest(prompt_text: str, cache_name: str, api_key: str) -> dict:
    url = f"{BASE_API_URL}/{MODEL_NAME}:generateContent?key={api_key}"
    body = {
        "cachedContent": cache_name,
        "contents": [{"role": "user", "parts": [{"text": prompt_text}]}],
        "generationConfig": {"temperature": 0.2, "responseMimeType": "application/json"}
    }
    resp = requests.post(url, headers={"Content-Type": "application/json"}, json=body, timeout=420)
    resp.raise_for_status()
    return resp.json()

# --- 5. Core Processing Worker ---
def process_group(
    group_to_process: Dict[str, Any],
    cde_lookup: Dict[str, Dict[str, Any]],
    cache_name: Optional[str],
    api_key: str
) -> Dict[str, Any]:
    group_id = group_to_process.get("group_id", "unknown_group")
    response_json = {}
    output_text = ""
    try:
        cde_group_data = []
        for cde_id in group_to_process.get("member_cde_ids", []):
            cde_details = cde_lookup.get(str(cde_id))
            if cde_details:
                value_field = {}
                if cde_details.get('value_mapping') is not None and pd.notna(cde_details.get('value_mapping')):
                    value_field['value_mapping'] = cde_details['value_mapping']
                elif cde_details.get('permissible_values') is not None and pd.notna(cde_details.get('permissible_values')):
                    value_field['permissible_values'] = cde_details['permissible_values']
                cde_group_data.append({
                    "ID": cde_details.get('ID'), "title": cde_details.get('title'),
                    "short_description": cde_details.get('short_description'),
                    "quality_flags": {"is_bad_variable_name": cde_details.get('flag_bad_variable_name', False)},
                    **value_field
                })
        if not cde_group_data:
            return {"group_id": group_id, "status": "no_valid_cdes", "data": [], "usage": None}
        
        prompt_text = json.dumps({"cde_group_for_review": cde_group_data})
        if not cache_name:
            raise ValueError("Cache not available for this group, cannot proceed.")
        
        response_json = generate_content_via_rest(prompt_text, cache_name, api_key)
        
        with open(os.path.join(RAW_DIR, f"{group_id}_response.json"), 'w', encoding='utf-8') as f:
            json.dump(response_json, f, indent=2)
            
        output_text = response_json.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
        if not output_text: raise ValueError("No text payload in API response.")
        validated_response = AIResponse.model_validate_json(output_text)
        
        return {
            "group_id": group_id, "status": "success",
            "data": [item.model_dump() for item in validated_response.root],
            "usage": response_json.get("usageMetadata")
        }
    except requests.exceptions.RequestException as e:
        log_error(group_id, e, {"stage": "rest_api_call_failed", "response": e.response.text if e.response else "N/A"})
        return {"group_id": group_id, "status": "api_error", "data": [], "usage": None}
    except ValidationError as e:
        log_error(group_id, e, {"stage": "validation_error", "details": output_text})
        return {"group_id": group_id, "status": "validation_error", "data": [], "usage": response_json.get("usageMetadata")}
    except Exception as e:
        log_error(group_id, e, {"stage": "unknown_worker_error"})
        return {"group_id": group_id, "status": "unknown_error", "data": [], "usage": None}

def run_single_test():
    """
    A simple function to test a single API call for a specific group
    without running the full pipeline orchestration.
    """
    # --- 1. CONFIGURATION: Choose a group to test ---
    TEST_GROUP_ID = "grp_0" # <-- Change this to the ID of the group you want to test
    logging.info(f"--- Running Single Test for Group: {TEST_GROUP_ID} ---")

    # --- 2. MINIMAL SETUP: Load necessary data ---
    cde_lookup = load_cde_catalog(PROCESSED_CATALOG_PATH)
    with open(INPUT_COMMUNITIES_PATH, 'r', encoding='utf-8') as f:
        communities = json.load(f)

    # --- 3. FIND THE TARGET GROUP AND ITS PARENT CONTEXT ---
    target_group = None
    community_context = ""
    for community in communities:
        for group in community.get("sub_groups", []):
            if group.get("group_id") == TEST_GROUP_ID:
                target_group = group
                # Get titles for the parent community context
                member_titles = [cde_lookup.get(str(id), {}).get('title', '') for id in community.get("member_cde_ids", [])]
                community_context = "\n- ".join(filter(None, member_titles))
                break
        if target_group:
            break
    
    if not target_group:
        logging.error(f"Could not find group with ID '{TEST_GROUP_ID}' in {INPUT_COMMUNITIES_PATH}")
        return

    # --- 4. EXECUTE A SINGLE CALL (NO CACHING) ---
    logging.info("Calling process_group directly for the test group...")
    # We call the core worker function directly, bypassing the parallel processing and caching logic
    result = process_group(
        group_to_process=target_group,
        community_context_text=community_context,
        cde_lookup=cde_lookup,
        cache=None # We pass None for the cache to test a single, non-cached API call
    )

    # --- 5. PRINT THE FULL RESULT ---
    print("\n" + "="*80)
    print("--- SINGLE TEST COMPLETE: RESULT ---")
    # Pretty-print the entire result dictionary to see the status, data, and token usage
    print(json.dumps(result, indent=2))
    print("="*80)

# --- 6. Main Orchestration Function ---

def main(api_key: str):
    """Main function updated to orchestrate the REST-based workflow."""
    logging.info("--- CDE Harmonization: Stage 3 Adjudication (REST API) ---")
    all_results = []; total_cost = 0.0
    
    cde_lookup = load_cde_catalog(PROCESSED_CATALOG_PATH)
    community_definitions = load_and_validate_communities(INPUT_COMMUNITIES_PATH, cde_lookup)
    if not community_definitions:
        logging.fatal("Pipeline halted."); return
    manifest = load_manifest(MANIFEST_FILE)
    
    for community in tqdm(community_definitions, desc="Processing Communities"):
        community_id = community.community_id
        cache_name = None # Use cache_name string for REST
        
        community_member_titles = [cde_lookup.get(str(id), {}).get('title', '') for id in community.member_cde_ids]
        community_context_text = "\n- ".join(filter(None, community_member_titles))

        groups_to_process = [g.dict() for g in community.sub_groups if manifest.get(g.group_id) != "success"]
        if not groups_to_process:
            logging.info(f"All groups in community {community_id} already processed. Skipping.")
            continue
        
        logging.info(f"Processing {len(groups_to_process)} groups for community {community_id}.")

        try:
            # FIX: Use the SDK's token counter for an accurate pre-flight check
            full_context_for_check = [SYSTEM_PROMPT, "### COMMUNITY CONTEXT ###\n" + community_context_text]
            token_count = genai.count_tokens(model=MODEL_NAME, contents=full_context_for_check).total_tokens

            # Check against the model's actual token limit (with a safety margin)
            # The limit for Gemini 2.5 Pro is large, but this is a robust check.
            if token_count < 1000000: 
                cache = create_cache(MODEL_NAME, SYSTEM_PROMPT, community_context_text)
            else:
                logging.warning(f"Community {community_id} context is too large ({token_count} tokens). Skipping cache creation.")

            with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_group = {
                    executor.submit(process_group, group, community_context_text, cde_lookup, cache): group
                    for group in groups_to_process
                }

                for future in tqdm(concurrent.futures.as_completed(future_to_group), total=len(groups_to_process), desc=f"Groups in {community_id}"):
                    group = future_to_group[future]
                    group_id = group.get("group_id", "unknown")
                    
                    try:
                        result = future.result()
                        status = result.get("status", "unknown_error")
                        manifest[group_id] = status
                        
                        if status == "success":
                            all_results.extend(result.get("data", []))
                            usage = result.get("usage")
                            if usage:
                                # CORRECTED: Accurate cost calculation for cached and non-cached tokens
                                cached_tokens = getattr(usage, 'cached_content_token_count', 0)
                                cost = (usage.prompt_token_count * TOKEN_PRICING["input"]) + \
                                       (usage.candidates_token_count * TOKEN_PRICING["output"]) + \
                                       (cached_tokens * TOKEN_PRICING["cached"])
                                total_cost += cost
                                log_token_usage(group_id, usage, cost)
                        
                        if total_cost >= COST_LIMIT_USD:
                            logging.fatal(f"COST LIMIT REACHED (${total_cost:.2f}). Halting.")
                            return

                    except Exception as exc:
                        manifest[group_id] = "future_failed"
                        log_error(group_id, exc)
            
            save_manifest(MANIFEST_FILE, manifest)

        finally:
            if cache:
                delete_cache(cache)
    
    logging.info(f"All communities processed. Saving {len(all_results)} total suggestions to {FINAL_RESULTS_PATH}")
    with open(FINAL_RESULTS_PATH, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, indent=2)

    logging.info(f"--- Stage 3 Adjudication Complete. Total Estimated Cost: ${total_cost:.4f} USD ---")



# --- 7. Main Execution Guard ---
if __name__ == "__main__":
    # Setup logging
    if os.path.exists(LOG_FILE): os.remove(LOG_FILE)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s",
        handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)]
    )

    # Configure API Key
    # NOTE: For improved security, consider using 'HF_HOME' for Hugging Face model caching.
    # export HF_HOME="/path/to/your/cache"
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        logging.fatal("FATAL: GOOGLE_API_KEY environment variable not found.")
        raise ValueError("GOOGLE_API_KEY must be set.")
    
    genai.configure(api_key=api_key)
    logging.info("Successfully configured Google GenAI with API key.")
    
    # Run the main pipeline
    # main()
    run_single_test()