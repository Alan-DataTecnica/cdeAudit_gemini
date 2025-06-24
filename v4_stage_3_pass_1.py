# stage_3_pass_1.py
# Purpose: Executes Pass 1 (Triage & Core Enrichment) of the CDE adjudication process.
# This script is designed to be resilient to Pydantic validation errors.

import os
import json
import logging
import pandas as pd
import concurrent.futures
from dotenv import load_dotenv
from tqdm import tqdm
from pydantic import ValidationError

# --- Import from the shared utility module ---
import shared_utils as utils

# --- 1. PASS 1: SYSTEM PROMPT ---
# This prompt is taken directly from the original v3_stage_3.py script.
SYSTEM_PROMPT_PASS_1 = """
### ROLE ###
You are a meticulous data architect and clinical data manager specializing in CDE (Common Data Element) harmonization for biomedical research.

### PRIMARY MISSION ###
Your mission is to perform a first-pass analysis on each CDE within the provided "CDE Group for Review". You will return a single, valid JSON array `[...]`, where each object corresponds to one CDE from the input group. Each object MUST contain ONLY two keys: "ID" and "suggestions". Your entire response MUST be only the JSON array, with no additional text or explanations.

### OUTPUT SCHEMA ###
Each object in the output array must contain two keys: "ID" (string) and "suggestions" (object).
**CRITICAL:** The "suggestions" object must be a flat key-value structure. Do not use nested JSON objects for any suggestion in this pass.

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
"""


# --- 2. WORKER FUNCTION ---

def process_group_pass_1(
    group_to_process: dict,
    community_context_text: str,
    cde_lookup: dict,
    cache_name: str,
    api_key: str
) -> dict:
    """
    Processes a single group for Pass 1, with robust error handling for Pydantic.
    """
    group_id = group_to_process.get("group_id", "unknown_group")
    
    try:
        # Construct the prompt payload for the API
        cde_group_data = []
        for cde_id in group_to_process.get("member_cde_ids", []):
            cde_details = cde_lookup.get(str(cde_id))
            if cde_details:
                cde_group_data.append({
                    "ID": cde_details.get('ID'),
                    "title": cde_details.get('title'),
                    "short_description": cde_details.get('short_description'),
                    "variable_name": cde_details.get('variable_name'),
                    "permissible_values": cde_details.get('permissible_values'),
                    "value_mapping": cde_details.get('value_mapping'),
                    "quality_flags": {"is_bad_variable_name": cde_details.get('flag_bad_variable_name', False)},
                })
        
        if not cde_group_data:
            return {"group_id": group_id, "status": "skipped_no_valid_cdes", "data": None, "usage": None}

        prompt_payload = {
            "group_id_for_request": group_id,
            "cde_group_for_review": cde_group_data,
            # Community context is not sent in the main prompt, it's in the cache
        }
        prompt_text = json.dumps(prompt_payload)
        
        # Call the API - This function saves the raw response before returning
        response_json = utils.generate_content_via_rest(prompt_text, cache_name, api_key, utils.RAW_DIR_PASS_1)
        output_text = response_json.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
        
        if not output_text:
            raise ValueError("No text payload in API response.")
            
        # --- RESILIENT Pydantic Validation ---
        try:
            validated_response = utils.AIResponsePass1.model_validate_json(output_text)
            return {
                "group_id": group_id,
                "status": "success",
                "data": [item.model_dump() for item in validated_response.root],
                "usage": response_json.get("usageMetadata")
            }
        except ValidationError as e:
            # Pydantic validation failed. Log the error and mark for review.
            utils.log_error(group_id, e, {"stage": "pydantic_validation_pass_1", "ai_output_text": output_text})
            return {"group_id": group_id, "status": "pydantic_validation_error", "data": None, "usage": response_json.get("usageMetadata")}

    except Exception as e:
        # Catch any other exception during processing
        utils.log_error(group_id, e, {"stage": "unknown_worker_error_pass_1"})
        return {"group_id": group_id, "status": "processing_error", "data": None, "usage": None}


# --- 3. MAIN ORCHESTRATION ---

def main(api_key: str):
    """Main function to run the complete Pass 1 adjudication process."""
    utils.setup_logging()
    logging.info("--- CDE Harmonization: STARTING Stage 3, Pass 1 (Triage & Enrichment) ---")
    
    # --- Load Inputs ---
    # File paths are relative to the root project directory
    community_definitions_path = os.path.join('outputs', 'stage_2', 'community_definitions.json')
    processed_catalog_path = os.path.join('outputs', 'stage_1', 'cde_catalog_processed.csv')
    manifest_path = os.path.join(utils.OUTPUT_DIR, "manifest_pass_1.json")

    try:
        with open(community_definitions_path, 'r') as f:
            community_definitions = [utils.ParentCommunity.model_validate(item) for item in json.load(f)]
        
        cde_df = pd.read_csv(processed_catalog_path, dtype={'ID': str}, low_memory=False)
        cde_lookup = cde_df.set_index('ID').to_dict('index')
    except Exception as e:
        logging.fatal(f"Could not load critical input files: {e}")
        return

    manifest = utils.load_manifest(manifest_path)
    total_cost = 0.0

    # --- Process Each Community ---
    for community in tqdm(community_definitions, desc="Processing Communities"):
        community_id = community.community_id
        cache_name = None
        
        # Define community context for the cache
        community_member_titles = [cde_lookup.get(str(id), {}).get('title', '') for id in community.member_cde_ids]
        community_context_text = "\n- ".join(filter(None, community_member_titles))

        groups_to_process = [g.model_dump() for g in community.sub_groups if manifest.get(g.group_id) != "success"]
        
        if not groups_to_process:
            logging.info(f"All groups in {community_id} already processed. Skipping.")
            continue
            
        logging.info(f"Processing {len(groups_to_process)} groups for community {community_id}.")

        try:
            # Create a short-lived cache for this community's context
            cache_name = utils.create_cache_via_rest(api_key, SYSTEM_PROMPT_PASS_1, community_context_text, utils.CACHE_DISPLAY_NAME_PASS_1)
            if not cache_name:
                logging.error(f"Failed to create cache for {community_id}. Skipping community.")
                continue

            with concurrent.futures.ThreadPoolExecutor(max_workers=utils.MAX_WORKERS) as executor:
                future_to_group = {
                    executor.submit(process_group_pass_1, group, community_context_text, cde_lookup, cache_name, api_key): group
                    for group in groups_to_process
                }

                for future in tqdm(concurrent.futures.as_completed(future_to_group), total=len(future_to_group), desc=f"Groups in {community_id}"):
                    group_id = future_to_group[future].get("group_id", "unknown")
                    try:
                        result = future.result()
                        status = result.get("status", "unknown_error")
                        manifest[group_id] = status
                        
                        # Log token usage if the API call was made
                        if result.get("usage"):
                            utils.log_token_usage(group_id, result["usage"], "pass_1")
                            
                    except Exception as exc:
                        manifest[group_id] = "future_failed"
                        utils.log_error(group_id, exc, {"note": "Error retrieving result from future."})

        finally:
            if cache_name:
                utils.delete_cache(cache_name, api_key)
            # Save manifest periodically
            utils.save_manifest(manifest_path, manifest)

    logging.info(f"--- Stage 3, Pass 1 COMPLETE ---")


if __name__ == "__main__":
    # Load the Google API key from the environment
    load_dotenv()
    google_api_key = os.getenv("GOOGLE_API_KEY")
    if not google_api_key:
        logging.fatal("FATAL: GOOGLE_API_KEY environment variable not found.")
    else:
        main(api_key=google_api_key)