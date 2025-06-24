# stage_3_pass_2.py
# Purpose: Executes Pass 2 (Specialized Value Mapping) of the CDE adjudication process.
# This script reads the results of Pass 1, processes only the CDEs that require it,
# and uses a specialized prompt to generate complex, nested value mappings.

import os
import json
import logging
import pandas as pd
import concurrent.futures
from dotenv import load_dotenv
from tqdm import tqdm
from pydantic import BaseModel, Field, ValidationError, RootModel
from typing import List, Dict, Any, Optional

# --- Import from the shared utility module ---
import shared_utils as utils

# --- 1. PYDANTIC MODELS for PASS 2 VALIDATION ---
# These models are defined here to validate the specific nested output required by Pass 2.

class ValueVariant(BaseModel):
    variant_description: str = Field(..., description="A description of this specific variant, e.g., 'Measured in mg/dL' or '15-point scale'.")
    permissible_values: Optional[str] = Field(None, description="Pipe-separated list of allowed values, e.g., '1|2|3|4|5'.")
    value_format: Optional[str] = Field(None, description="The data type, e.g., 'Integer', 'String', 'Decimal'.")
    unit_of_measure: Optional[str] = Field(None, description="The unit for this variant, e.g., 'mg/dL', 'years'.")
    preferred_question_text: str = Field(..., description="The ideal question text to elicit a response for this specific variant.")

class ValueMapping(BaseModel):
    variants: List[ValueVariant]

class SuggestionPass2(BaseModel):
    value_mapping: ValueMapping

class AdjudicationResultPass2(BaseModel):
    ID: str
    suggestions: SuggestionPass2

class AIResponsePass2(RootModel[List[AdjudicationResultPass2]]):
    pass


# --- 2. PASS 2: SYSTEM PROMPT ---
# This prompt is specifically designed to guide the AI in generating complex, nested JSON.

SYSTEM_PROMPT_PASS_2 = """
### ROLE ###
You are an expert-level data architect and clinical data manager. Your mission is to define complex value mappings for a list of Common Data Elements (CDEs) that have been flagged as needing advanced review.

### PRIMARY MISSION ###
For each CDE provided, you will generate a `value_mapping` object. This object will contain one or more "variants" to handle different units, response scales, or methods of data collection. Your entire response MUST be a single, valid JSON array `[...]`, where each object corresponds to one CDE from the input.

### OUTPUT SCHEMA (CRITICAL) ###
You must adhere strictly to the following nested JSON structure for each CDE in your response array.

```json
[
  {
    "ID": "The CDE's unique identifier (string)",
    "suggestions": {
      "value_mapping": {
        "variants": [
          {
            "variant_description": "A human-readable description of this variant (e.g., 'Response on a 5-point Likert scale' or 'Measurement in US customary units').",
            "permissible_values": "A pipe-separated string of allowed values if applicable (e.g., '1|2|3|4|5'). Can be null for free-text or continuous values.",
            "value_format": "The data type for this variant (e.g., 'Integer', 'Decimal', 'String').",
            "unit_of_measure": "The specific unit for this variant (e.g., 'points', 'inches', 'mg/dL'). Can be null.",
            "preferred_question_text": "The ideal, clinical-grade question text to capture data for THIS SPECIFIC variant."
          }
        ]
      }
    }
  }
]
```

### EXAMPLE (FEW-SHOT) ###
Here is an example for a CDE measuring height. Notice the two variants for metric and imperial units.

**INPUT CDE:** `{"ID": "CDE_12345", "title": "Height Measurement", "short_description": "The measurement of a person's vertical distance from head to foot."}`

**CORRECT OUTPUT:**
```json
[
  {
    "ID": "CDE_12345",
    "suggestions": {
      "value_mapping": {
        "variants": [
          {
            "variant_description": "Height measured in centimeters.",
            "permissible_values": null,
            "value_format": "Decimal",
            "unit_of_measure": "cm",
            "preferred_question_text": "What is the subject's height in centimeters (cm)?"
          },
          {
            "variant_description": "Height measured in inches.",
            "permissible_values": null,
            "value_format": "Decimal",
            "unit_of_measure": "in",
            "preferred_question_text": "What is the subject's height in inches (in)?"
          }
        ]
      }
    }
  }
]
```

### SPECIAL INSTRUCTIONS ###
- For standard clinical instruments (e.g., MMSE, GDS, MoCA), ensure the `permissible_values`, `value_format`, and `unit_of_measure` adhere to the official definition of the instrument.
- If a CDE already has a clear, unambiguous value definition, you may create a single variant that standardizes it.
- Your entire response MUST be only the JSON array. Do not include any other text, explanations, or markdown.
"""


# --- 3. CORE LOGIC FUNCTIONS ---

def aggregate_and_filter_pass_1_results(pass_1_dir: str) -> List[str]:
    """
    Parses all Pass 1 results, filtering for CDEs needing advanced review.
    """
    logging.info("Aggregating and filtering results from Pass 1...")
    cdes_for_pass_2 = []
    
    if not os.path.exists(pass_1_dir):
        logging.error(f"Pass 1 output directory not found: {pass_1_dir}")
        return []

    pass_1_files = [f for f in os.listdir(pass_1_dir) if f.endswith(".json")]
    for filename in tqdm(pass_1_files, desc="Aggregating Pass 1 Results"):
        filepath = os.path.join(pass_1_dir, filename)
        with open(filepath, 'r', encoding='utf-8') as f:
            try:
                response_data = json.load(f)
                content_text = response_data.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
                if not content_text:
                    continue
                
                suggestions_list = json.loads(content_text)
                
                for suggestion in suggestions_list:
                    if suggestion.get("suggestions", {}).get("requires_advanced_value_review") is True:
                        cdes_for_pass_2.append(suggestion["ID"])
            except (json.JSONDecodeError, KeyError, IndexError, TypeError) as e:
                logging.warning(f"Could not parse or find data in Pass 1 response file: {filename}. Error: {e}")

    logging.info(f"Found {len(cdes_for_pass_2)} CDEs flagged for advanced value review in Pass 2.")
    return list(set(cdes_for_pass_2)) # Return unique IDs


def create_pass_2_batches(cdes_to_process: List[str], community_definitions: List[Dict], cde_lookup: Dict) -> List[Dict]:
    """Groups the filtered CDEs into new batches for Pass 2 processing."""
    logging.info("Creating new batches for Pass 2 processing...")
    community_map = {str(cde_id): comm['community_id'] for comm in community_definitions for cde_id in comm['member_cde_ids']}
    
    community_groups = {}
    for cde_id in cdes_to_process:
        comm_id = community_map.get(cde_id)
        if comm_id:
            community_groups.setdefault(comm_id, []).append(cde_id)
            
    pass_2_batches = []
    group_counter = 0
    BATCH_SIZE_PASS_2 = 25

    for comm_id, cde_ids in community_groups.items():
        for i in range(0, len(cde_ids), BATCH_SIZE_PASS_2):
            batch_cde_ids = cde_ids[i:i + BATCH_SIZE_PASS_2]
            batch_data = [cde_lookup[cde_id] for cde_id in batch_cde_ids if cde_id in cde_lookup]
            
            pass_2_batches.append({
                "group_id": f"p2_grp_{group_counter}",
                "community_id": comm_id,
                "cde_data": batch_data
            })
            group_counter += 1
            
    logging.info(f"Created {len(pass_2_batches)} new batches for Pass 2.")
    return pass_2_batches


def process_group_pass_2(batch: dict, community_context_map: dict, api_key: str) -> dict:
    """Processes a single batch for Pass 2."""
    group_id = batch["group_id"]
    community_id = batch["community_id"]
    community_context = community_context_map.get(community_id, "")
    cache_name = None
    
    try:
        cache_name = utils.create_cache_via_rest(api_key, SYSTEM_PROMPT_PASS_2, community_context, utils.CACHE_DISPLAY_NAME_PASS_2)
        if not cache_name:
            raise Exception("Failed to create cache for Pass 2.")

        # For Pass 2, the prompt only needs the CDEs to be processed
        prompt_payload = {
            "group_id_for_request": group_id,
            "cde_group_for_review": batch["cde_data"],
        }
        prompt_text = json.dumps(prompt_payload)
        
        response_json = utils.generate_content_via_rest(prompt_text, cache_name, api_key, utils.RAW_DIR_PASS_2)
        output_text = response_json.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
        if not output_text:
            raise ValueError("No text payload in API response.")

        try:
            AIResponsePass2.model_validate_json(output_text)
            return {"group_id": group_id, "status": "success", "usage": response_json.get("usageMetadata")}
        except ValidationError as e:
            utils.log_error(group_id, e, {"stage": "pydantic_validation_pass_2", "ai_output_text": output_text})
            return {"group_id": group_id, "status": "pydantic_validation_error", "usage": response_json.get("usageMetadata")}

    except Exception as e:
        utils.log_error(group_id, e, {"stage": "unknown_worker_error_pass_2"})
        return {"group_id": group_id, "status": "processing_error", "usage": None}
    finally:
        if cache_name:
            utils.delete_cache(cache_name, api_key)


# --- 4. MAIN ORCHESTRATION ---

def main(api_key: str):
    """Main function to run the complete Pass 2 adjudication process."""
    utils.setup_logging()
    logging.info("--- CDE Harmonization: STARTING Stage 3, Pass 2 (Specialized Value Mapping) ---")

    # --- Load Supporting Files ---
    community_definitions_path = os.path.join('outputs', 'stage_2', 'community_definitions.json')
    processed_catalog_path = os.path.join('outputs', 'stage_1', 'cde_catalog_processed.csv')
    manifest_path = os.path.join(utils.OUTPUT_DIR, "manifest_pass_2.json")

    try:
        with open(community_definitions_path, 'r', encoding='utf-8') as f:
            community_definitions = json.load(f)
        
        cde_df = pd.read_csv(processed_catalog_path, dtype={'ID': str}, low_memory=False)
        cde_lookup = cde_df.set_index('ID').to_dict('index')

        community_context_map = {
            comm['community_id']: "\n- ".join(filter(None, [cde_lookup.get(str(id), {}).get('title', '') for id in comm['member_cde_ids']]))
            for comm in community_definitions
        }
    except Exception as e:
        logging.fatal(f"Could not load critical input files: {e}")
        return

    # --- Step 1: Aggregate and Batch ---
    cdes_to_process = aggregate_and_filter_pass_1_results(utils.RAW_DIR_PASS_1)
    if not cdes_to_process:
        logging.info("No CDEs were flagged for Pass 2 review. Stage complete.")
        return
        
    pass_2_batches = create_pass_2_batches(cdes_to_process, community_definitions, cde_lookup)
    manifest = utils.load_manifest(manifest_path)
    batches_to_process = [b for b in pass_2_batches if manifest.get(b['group_id']) != "success"]
    
    if not batches_to_process:
        logging.info("All required Pass 2 batches have already been processed successfully.")
        return

    # --- Step 2: Execute Pass 2 in Parallel ---
    with concurrent.futures.ThreadPoolExecutor(max_workers=utils.MAX_WORKERS) as executor:
        future_to_batch = {
            executor.submit(process_group_pass_2, batch, community_context_map, api_key): batch
            for batch in batches_to_process
        }
        
        for future in tqdm(concurrent.futures.as_completed(future_to_batch), total=len(future_to_batch), desc="Processing Pass 2 Batches"):
            batch_id = future_to_batch[future]["group_id"]
            try:
                result = future.result()
                status = result.get("status", "unknown_error")
                manifest[batch_id] = status

                if result.get("usage"):
                    utils.log_token_usage(batch_id, result["usage"], "pass_2")
            
            except Exception as exc:
                manifest[batch_id] = "future_failed"
                utils.log_error(batch_id, exc, {"note": "Error retrieving result from future."})
            
            finally:
                utils.save_manifest(manifest_path, manifest)

    logging.info("--- Stage 3, Pass 2 COMPLETE ---")


if __name__ == "__main__":
    load_dotenv()
    google_api_key = os.getenv("GOOGLE_API_KEY")
    if not google_api_key:
        logging.fatal("FATAL: GOOGLE_API_KEY environment variable not found.")
    else:
        main(api_key=google_api_key)
