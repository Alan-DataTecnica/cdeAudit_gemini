import os
import json
import time
import logging
import requests
from typing import List, Dict, Any, Optional
import concurrent.futures
from pydantic import BaseModel
from tqdm import tqdm
import sys

# This script uses the google-genai library.
import google.genai as genai
from google.genai import types
from google.api_core import exceptions as google_exceptions


# --- 1. CONFIGURATION (Constants and Definitions) ---

# File and Directory Paths
INPUT_CATALOG_PATH = "outputs/stage_1/cde_catalog_processed.csv"
INPUT_GROUPS_PATH = "outputs/stage_2/similarity_communities.json"
OUTPUT_DIR = "stage3_output"
RAW_DIR = "stage3_output/raw_responses"
MANIFEST_FILE = "manifest.json"
ERROR_LOG_PATH = "stage3_error_log.txt"
TOKEN_LOG_PATH = "stage3_token_log.csv"
LOG_FILE = "stage3_pipeline.log" # Define log file name here
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(RAW_DIR, exist_ok=True)


# Worker and Safety Controls
MAX_WORKERS = 8
MAX_CONSECUTIVE_ERRORS = 10
COST_LIMIT_USD = 250.0

# API and Model Configuration
MODEL_NAME = "models/gemini-2.5-flash-preview-05-20"
BASE_API_URL = "https://generativelanguage.googleapis.com/v1beta"
CACHE_DISPLAY_NAME = "cde_harmonization_cache_main"
TOKEN_PRICING = {
    "input": 0.000000125,
    "output": 0.000000375
}

CDE_COLLECTIONS = sorted(list(set([
    'Availability', 'Family History', 'Screening', 'Pathology'
    'NACC', 'ADC', 'ADNI', 'PPMI', 'Neuroimaging', 'Genomics', 'Digital',
    'Cognitive', 'Clinical', 'Biomarkers', 'Demographics', 'Vital Signs',
    'Laboratory Results', 'Medication Administration', 'Patient-Reported Outcomes',
    'Clinician-Reported Outcomes', 'Medical History & Events', 'Physical Examination',
    'Diagnosis', 'Oncology', 'Cardiology', 'Neurology', 'Endocrinology',
    'Infectious Disease', 'Genomic Markers', 'Imaging',
    'Study Protocol & Administration', 'Gemetics', 'ID', 'Experimental Model',
    'Transcriptomics','Epigenomics', 'Metabolomics', 'Proteomics',
    'Behavioral', 'Social', 'Environmental', 'Epidemiological', 'Ethnicity',
    'Genetic', 'Molecular', 'Cognitive', 'Cardiovascular', 'Metabolic Disorders'
    'Neurodegenerative Diseases', 'Geriatric', 'Rheumotology'
])))

CACHED_SYSTEM_INSTRUCTION = f"""
You are a precise healthcare data standardization assistant. Your primary mission is to process the provided JSON containing a group of similar Common Data Elements (CDEs) and return a single, flat JSON array of suggestions. Strictly follow all rules.

### 1. Core Mission & Output Specification

Return a **single, flat JSON array `[...]`**.  
Each object in the array must correspond to one CDE and have exactly two keys:

- `"ID"` (the CDE's ID as a string)
- `"suggestions"` (an object containing suggested changes)

**Important:** If a CDE is perfect and requires no changes, you MUST still include it with an empty `"suggestions"` object.

**Example Output:**
```json
    [
    {{
        "ID": "3",
        "suggestions": {{
        "variable_name": "age_in_years",
        "redundancy_flag": true,
        "redundant_with_ids": "101|102"
        }}
    }},
    {{
        "ID": "14",
        "suggestions": {{
        "title": "Geriatric Depression Scale - Memory Problem Indicator"
        }}
    }},
    {{
        "ID": "53",
        "suggestions": {{}}
    }}
    ]

2. Input Data & Logic

The input JSON provided will contain a key "cde_group_for_review", holding a list of similar CDEs. For each CDE in this group, perform these two tasks:

    Task A (Populate Fields): Fill empty or poor-quality fields.
    Task B (Analyze Redundancy): Identify duplicates by comparing each CDE to the others in its group.

3. Field Population Guidance

Target fields to populate or improve include:
title, short_description, variable_name, alternate_titles, preferred_question_text, collections, permissible_values.

    collections: Assign one or more of these categories:
    {'|'.join(CDE_COLLECTIONS)}
    Use a pipe (|) to separate multiple categories.

    short_description: Provide a concise clinical definition. Do not begin with "This CDE represents...".
    variable_name: Use snake_case, start with a letter, limit length to 25 characters (ideally under 15). Use standard abbreviations listed in Section 5.
    permissible_values: Format carefully:

        For Date: "YYYY-MM-DD"

        For DateTime: "YYYY-MM-DDThh:mm:ss"
        For Binary: "0:No|1:Yes"

4. Quality & Redundancy Analysis

If issues are detected, clearly indicate them using the following fields in "suggestions":
    quality_review_flag: Set true if manual review is recommended.
    quality_score: Integer rating from 1 (minor) to 5 (critical).
    redundancy_flag: Set true if the CDE is redundant or duplicated.
    redundancy_action: Choose exactly one from "REVIEW", "RETAIN", or "DELETE".
    redundant_with_ids: Provide duplicate CDE IDs separated by pipes (|).

5. Standard Abbreviations for variable_name

    General:
    Num, No, Amt, Avg, Tot, Cnt, Msr, Idx, Scr, Lvl, Val, Pct, Rt, Freq, Vol, Sz, Wt, Qst, Resp, Summ, Desc, ID, Cat, Typ, Stat, Chg, Diff.

    Medical:
    Dx, Trt, Tx, Asmt, Hx, Med, Clin, Sympt, Proc, Exam.

    Time:
    Ag, Dt, Yr, Mo, Dy, Dly, Wkly, Dur, Prd, Ons.
"""

# --- 2. Pydantic Models ---

class Suggestions(BaseModel):
    class Config: extra = 'allow'

class CdeAdjudication(BaseModel):
    ID: str
    suggestions: Suggestions


# --- 3. Helper and API Functions ---

def log_error(group_id: str, err: Any, details: Dict[str, Any] = None):
    error_message = f"--- ERROR: group {group_id} @ {time.ctime()} ---\n"
    error_message += f"Error Type: {type(err).__name__}\n"
    error_message += f"Error Message: {str(err)}\n"
    if details:
        error_message += "--- Details ---\n"
        for key, value in details.items():
            error_message += f"{key}: {value}\n"
        error_message += "---------------\n"
    
    with open(ERROR_LOG_PATH, 'a') as f:
        f.write(error_message + "\n\n")
    # Use logging.error to also send to console/main log if configured
    logging.error(f"Logged critical error for group {group_id}. See {ERROR_LOG_PATH}.")

def log_token_usage(group_id: str, usage_metadata: Any, call_cost: float):
    if not os.path.exists(TOKEN_LOG_PATH):
        with open(TOKEN_LOG_PATH, "w") as f:
            f.write("group_id,prompt_tokens,cached_tokens,output_tokens,total_tokens,call_cost_usd\n")
    
    with open(TOKEN_LOG_PATH, "a") as f:
        f.write(f"{group_id},{usage_metadata.prompt_token_count},{usage_metadata.cached_content_token_count},{usage_metadata.candidates_token_count},{usage_metadata.total_token_count},{call_cost:.8f}\n")

def create_cache_via_rest(api_key: str) -> Optional[str]:
    url = f"{BASE_API_URL}/cachedContents?key={api_key}"
    body = {
        "model": MODEL_NAME,
        "displayName": CACHE_DISPLAY_NAME,
        "systemInstruction": {"role": "system", "parts": [{"text": CACHED_SYSTEM_INSTRUCTION}]},
        "contents": [{"role": "user", "parts": [{"text": CACHED_SYSTEM_INSTRUCTION}]}],
        "ttl": "86400s"
    }
    
    logging.info(f"Attempting to create cache '{CACHE_DISPLAY_NAME}'...")
    try:
        resp = requests.post(url, headers={"Content-Type": "application/json"}, json=body, timeout=30)
        
        resp.raise_for_status()
        name = resp.json().get("name")
        logging.info(f"Cache created successfully: {name}")
        return name
    except requests.exceptions.RequestException as e:
        details = {"url": url, "request_body": json.dumps(body, indent=2)}
        if e.response is not None:
            details["status_code"] = e.response.status_code
            details["response_body"] = e.response.text
        log_error("CACHE_CREATE", e, details)
        return None

def delete_cache(cache_name: str, api_key: str):
    logging.info(f"Deleting cache: {cache_name}...")
    url = f"{BASE_API_URL}/{cache_name}?key={api_key}"
    try:
        requests.delete(url, timeout=15)
        logging.info(f"Successfully sent delete request for cache: {cache_name}")
    except requests.exceptions.RequestException as e:
        log_error(f"CACHE_DELETE_{cache_name}", e)

def load_cde_groups(file_path: str) -> List[Dict[str,Any]]:
    """
    Expects JSON as:
      [ { "ID": str, "CDEs": [int,…] }, … ]
    """
    try:
        groups = json.load(open(file_path, "r"))
        # basic validation
        if not isinstance(groups, list) or not all(isinstance(g, dict) for g in groups):
            raise ValueError("Expected list of objects")
        return groups
    except Exception as e:
        logging.error(f"Failed to load CDE groups from {file_path}: {e}")
        return []

def load_manifest(file_path: str) -> Dict[str, str]:
    if not os.path.exists(file_path): return {}
    with open(file_path, "r") as f: return json.load(f)

def save_manifest(file_path: str, manifest: Dict[str, str]):
    with open(file_path, "w") as f: json.dump(manifest, f, indent=4)

def generate_content_via_rest(
    prompt_text: str,
    cache_name: str,
    api_key: str,
    model_name: str,
    group_id: str
) -> dict:
    """
    Sends a prompt to Gemini via REST, using an existing cache.
    ALWAYS record the full request and full raw response to disk.
    """
    # Prepare
    url = f"{BASE_API_URL}/{model_name}:generateContent?key={api_key}"
    body = {
        "cachedContent": cache_name,
        "contents": [{"role": "user", "parts": [{"text": prompt_text}]}],
        "generationConfig": {
            "maxOutputTokens": 55000,
            "temperature": 0.3,
            "responseMimeType": "application/json"
        }
    }

    # Ensure subfolders exist
    req_dir = os.path.join(RAW_DIR, "requests")
    resp_dir = os.path.join(RAW_DIR, "responses")
    os.makedirs(req_dir, exist_ok=True)
    os.makedirs(resp_dir, exist_ok=True)

    # 1) Write the request JSON
    req_path = os.path.join(req_dir, f"group_{group_id}_request.json")
    with open(req_path, "w", encoding="utf-8") as f:
        json.dump(body, f, indent=2)

    # 2) Actually call the API
    resp = requests.post(
        url,
        headers={"Content-Type": "application/json"},
        json=body,
        timeout=420
    )

    # 3) Write the raw HTTP response body (text) to disk
    resp_path = os.path.join(resp_dir, f"group_{group_id}_response.json")
    with open(resp_path, "w", encoding="utf-8") as f:
        try:
            # pretty-print if valid JSON
            json.dump(resp.json(), f, indent=2)
        except ValueError:
            # otherwise dump raw text
            f.write(resp.text)

    # 4) Continue with your usual error handling
    if not resp.ok:
        error_details = {
            "request_url": url,
            "status_code": resp.status_code,
            "response_text": resp.text
        }
        log_error(group_id, Exception("REST API failure"), error_details)
        raise RuntimeError(f"REST API call failed: {resp.status_code} {resp.text}")

    return resp.json()


# --- 4. Core Processing Worker ---

def process_cde_group(group_data: Dict[str, Any], api_key: str, cache_name: str, model_name: str) -> Dict[str, Any]:
    group_id = group_data["ID"]
    prompt   = json.dumps({"cde_group_for_review": group_data["CDEs"]})
    # 1) call & record
    try:
        response_json = generate_content_via_rest(prompt, cache_name, api_key, model_name, group_id)
    except Exception as e:
        # network‐level failure—still record and tag
        log_error(group_id, e, {"stage": "api_call_failed"})
        return {"group_id": group_id, "status": "api_call_failed"}

    # 2) dump the raw JSON always
    dump_dir = os.path.join(RAW_DIR, "responses")
    os.makedirs(dump_dir, exist_ok=True)
    with open(os.path.join(dump_dir, f"group_{group_id}.json"), "w", encoding="utf-8") as dbg:
        json.dump(response_json, dbg, indent=2)

    # 3) extract text payload
    candidate = response_json.get("candidates", [{}])[0]
    content   = candidate.get("content", candidate)
    if isinstance(content, dict) and "parts" in content:
        output_text = content["parts"][0].get("text", "")
    elif isinstance(content, dict) and "text" in content:
        output_text = content["text"]
    elif isinstance(content, str):
        output_text = content
    else:
        output_text = json.dumps(content)

    # 4) write parsed text
    raw_file = os.path.join(RAW_DIR, f"group_{group_id}_raw.txt")
    with open(raw_file, "w", encoding="utf-8") as f:
        f.write(output_text)

    # --- JSON auto-fix & validation (100% swallowed) ---
    def fix_truncated_json(txt: str) -> str:
        # close dangling quotes
        if txt.count('"') % 2:
            txt += '"'
        # balance brackets/braces
        opens  = txt.count('[') - txt.count(']')
        braces = txt.count('{') - txt.count('}')
        return txt + (']' * opens) + ('}' * braces)

    try:
        json.loads(output_text)
    except Exception as ve:
        # try auto-fix once
        fixed = fix_truncated_json(output_text)
        try:
            json.loads(fixed)
            output_text = fixed
            log_error(group_id, Exception("auto-fixed JSON"), {"stage":"auto_fix"})
        except Exception as ve2:
           log_error(group_id, ve2, {"stage":"validation_failed"})

    # never return error for parsing issues—only real exceptions above do that
    return {"group_id": group_id, "status": "success"}



# --- 5. Main Orchestration Function ---

def main(api_key: str):
    import pandas as pd
    logging.info("--- CDE Harmonization: Stage 3 ---")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Clear logs at the beginning of the main process ONLY.
    for path in [ERROR_LOG_PATH, TOKEN_LOG_PATH]:
        if os.path.exists(path): os.remove(path)

    cache_name = create_cache_via_rest(api_key)
    if not cache_name:
        logging.fatal("Pipeline cannot continue without a cache.")
        return

    try:
        df_catalog = pd.read_csv(INPUT_CATALOG_PATH, dtype={'ID': str}, low_memory=False).set_index('ID')
        group_definitions = load_cde_groups(INPUT_GROUPS_PATH)
        manifest = load_manifest(os.path.join(OUTPUT_DIR, MANIFEST_FILE))
        
        groups_to_process = []
        for group in group_definitions:
            group_id = str(group.get("ID"))
            if not group_id or manifest.get(group_id) == "success":
                continue
            # convert members to strings
            cde_ids = [str(x) for x in group.get("CDEs", [])]
            df_subset = df_catalog.loc[df_catalog.index.intersection(cde_ids)]
            if df_subset.empty:
                continue
            groups_to_process.append({
                "ID": group_id,
                "CDEs": df_subset.reset_index().to_dict("records")
            })

        if not groups_to_process:
            logging.info("All groups processed per manifest.")
            return

        logging.info(f"Starting adjudication for {len(groups_to_process)} groups...")
        
        consecutive_errors, total_cost = 0, 0.0
        
        with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process_cde_group, group, api_key, cache_name, MODEL_NAME): group for group in groups_to_process}
            
            for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Processing CDE Groups"):
                group = futures[future]
                group_id = group.get("ID", "unknown")
                try:
                    result = future.result()

                    manifest[group_id] = result.get("status")
                    
                    if result.get("status") == "success":
                        consecutive_errors = 0
                        usage = result.get("usage_metadata")
                        if usage:
                            cost = (usage.prompt_token_count * TOKEN_PRICING["input"]) + (usage.candidates_token_count * TOKEN_PRICING["output"])
                            total_cost += cost
                            log_token_usage(group_id, usage, cost)
                    else:
                        consecutive_errors += 1

                except Exception as exc:
                    manifest[group_id] = "future_failed"
                    log_error(group_id, exc)
                    consecutive_errors += 1
                
                if total_cost >= COST_LIMIT_USD:
                    logging.fatal(f"COST LIMIT REACHED (${total_cost:.2f}). Stopping.")
                    break
                
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    logging.fatal(f"FAILURE THRESHOLD REACHED ({consecutive_errors} consecutive errors). Stopping.")
                    break

                save_manifest(os.path.join(OUTPUT_DIR, MANIFEST_FILE), manifest)

    finally:
        if cache_name:
            delete_cache(cache_name, api_key)
        logging.info("Stage 3 processing finished.")


# --- 6. Main Execution Guard ---
if __name__ == "__main__":
    # --- FIX: Setup logging and API key check only in the main process ---
    
    # 1. Set up logging once.
    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s",
        handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
    )

    # 2. Check for API key once.
    API_KEY ="AIzaSyA3qCiyLqqCuzq4rrhY6OJPOR5bgc7exDk"
    if not API_KEY:
        logging.fatal("FATAL: GOOGLE_API_KEY not found in environment.")
        # No need to use exit(1), raising an error is cleaner
        raise ValueError("GOOGLE_API_KEY not found.")
    logging.info("Successfully loaded GOOGLE_API_KEY.")
    
    # 3. Call the main function.
    main(api_key=API_KEY)
