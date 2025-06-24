# stage_3_test_harness.py
# Purpose: A dedicated script to isolate and test a single API call for either
# Pass 1 or Pass 2 of the Stage 3 adjudication process. It provides detailed
# debug output including the exact request, response, timing, and cost.

import os
import json
import logging
import pandas as pd
from dotenv import load_dotenv
import time

# --- Import from the shared utility module ---
import shared_utils as utils
# --- Import prompts from the main scripts ---
from v4_stage_3_pass_1 import SYSTEM_PROMPT_PASS_1
from v4_stage_3_pass_2 import SYSTEM_PROMPT_PASS_2, AIResponsePass2, aggregate_and_filter_pass_1_results, create_pass_2_batches

def find_group_in_communities(group_id: str, community_definitions: list) -> (dict, dict):
    """Finds a specific group and its parent community from the definitions file."""
    for comm in community_definitions:
        for group in comm['sub_groups']:
            if group['group_id'] == group_id:
                return group, comm
    return None, None

def calculate_cost(usage_metadata: dict) -> float:
    """Calculates the cost of a single API call based on usage metadata."""
    prompt_tokens = usage_metadata.get('promptTokenCount', 0)
    output_tokens = usage_metadata.get('candidatesTokenCount', 0)
    cached_tokens = usage_metadata.get('cachedContentTokenCount', 0) # Note: Often 0 if context is small

    cost = (prompt_tokens * utils.TOKEN_PRICING["input"]) + \
           (output_tokens * utils.TOKEN_PRICING["output"]) + \
           (cached_tokens * utils.TOKEN_PRICING["cached"])
    return cost

def run_test(api_key: str, pass_number: int, group_id: str):
    """
    Main function to execute a single test run for a specified group and pass.
    """
    utils.setup_logging()
    logging.info(f"--- STARTING SINGLE TEST (Pass {pass_number}, Group: {group_id}) ---")

    # --- 1. Load Common Files ---
    community_definitions_path = os.path.join('outputs', 'stage_2', 'community_definitions.json')
    processed_catalog_path = os.path.join('outputs', 'stage_1', 'cde_catalog_processed.csv')

    try:
        with open(community_definitions_path, 'r') as f:
            community_definitions = [utils.ParentCommunity.model_validate(item) for item in json.load(f)]
        
        cde_df = pd.read_csv(processed_catalog_path, dtype={'ID': str}, low_memory=False)
        # --- FIX: Ensure title column is treated as string to prevent type errors ---
        cde_df['title'] = cde_df['title'].astype(str).fillna('')
        cde_lookup = cde_df.set_index('ID').to_dict('index')
    except Exception as e:
        logging.fatal(f"Could not load critical input files: {e}")
        return

    # --- 2. Prepare Payload and Context based on Pass Number ---
    prompt_payload = {}
    community_context_text = ""
    system_prompt = ""
    cache_display_name = ""
    raw_response_dir = ""

    if pass_number == 1:
        target_group, parent_community = find_group_in_communities(group_id, [c.model_dump() for c in community_definitions])
        if not target_group:
            logging.error(f"Could not find group_id '{group_id}' in community definitions.")
            return

        # --- FIX: Explicitly cast title to string during list comprehension ---
        community_member_titles = [str(cde_lookup.get(str(id), {}).get('title', '')) for id in parent_community['member_cde_ids']]
        community_context_text = "\n- ".join(filter(None, community_member_titles))
        
        cde_group_data = [cde_lookup.get(str(cde_id)) for cde_id in target_group.get("member_cde_ids", []) if str(cde_id) in cde_lookup]

        prompt_payload = {
            "group_id_for_request": group_id,
            "cde_group_for_review": cde_group_data,
        }
        system_prompt = SYSTEM_PROMPT_PASS_1
        cache_display_name = f"test_cache_{group_id}"
        raw_response_dir = utils.RAW_DIR_PASS_1

    elif pass_number == 2:
        # For Pass 2, we need to find the specific CDEs that would be in the test group
        cdes_to_process = aggregate_and_filter_pass_1_results(utils.RAW_DIR_PASS_1)
        pass_2_batches = create_pass_2_batches(cdes_to_process, [c.model_dump() for c in community_definitions], cde_lookup)
        
        target_batch = next((batch for batch in pass_2_batches if batch['group_id'] == group_id), None)
        if not target_batch:
            logging.error(f"Could not find or create a Pass 2 batch with group_id '{group_id}'.")
            logging.error("Ensure Pass 1 has run and this group_id is valid for Pass 2.")
            return
            
        parent_community_id = target_batch['community_id']
        community_context_map = {
            # --- FIX: Explicitly cast title to string here as well ---
            comm.community_id: "\n- ".join(filter(None, [str(cde_lookup.get(str(id), {}).get('title', '')) for id in comm.member_cde_ids]))
            for comm in community_definitions
        }
        community_context_text = community_context_map.get(parent_community_id, "")

        prompt_payload = {
            "group_id_for_request": group_id,
            "cde_group_for_review": target_batch["cde_data"],
        }
        system_prompt = SYSTEM_PROMPT_PASS_2
        cache_display_name = f"test_cache_{group_id}"
        raw_response_dir = utils.RAW_DIR_PASS_2

    else:
        logging.error("Invalid pass number specified. Must be 1 or 2.")
        return

    # --- 3. Execute the Test ---
    cache_name = None
    try:
        # Create cache for the test
        cache_name = utils.create_cache_via_rest(api_key, system_prompt, community_context_text, cache_display_name)
        if not cache_name:
            raise Exception("Test failed: Cache creation returned None.")

        # Prepare the final request body for display
        prompt_text_for_api = json.dumps(prompt_payload)
        final_request_body = {
            "cachedContent": cache_name,
            "contents": [{"role": "user", "parts": [{"text": prompt_text_for_api}]}],
            "generationConfig": {"temperature": 0.2, "responseMimeType": "application/json"}
        }

        print("\n" + "="*80)
        print("--- API REQUEST PAYLOAD (What the API receives) ---")
        print(json.dumps(final_request_body, indent=2))
        print("="*80 + "\n")

        # Make the API call and measure time
        start_time = time.time()
        response_json = utils.generate_content_via_rest(prompt_text_for_api, cache_name, api_key, raw_response_dir)
        end_time = time.time()
        time_taken = end_time - start_time

        print("\n" + "="*80)
        print("--- API RESPONSE (Exact response from the API) ---")
        print(json.dumps(response_json, indent=2))
        print("="*80 + "\n")
        
        # --- 4. Display Timings and Cost ---
        usage = response_json.get("usageMetadata", {})
        cost = calculate_cost(usage)

        print("\n" + "="*80)
        print("--- TEST SUMMARY ---")
        print(f"Time Taken for API call: {time_taken:.2f} seconds")
        print(f"Prompt Tokens: {usage.get('promptTokenCount', 0)}")
        print(f"Output Tokens: {usage.get('candidatesTokenCount', 0)}")
        print(f"Total Tokens: {usage.get('totalTokenCount', 0)}")
        print(f"Estimated Cost for this call: ${cost:.8f} USD")
        print("="*80 + "\n")

    except Exception as e:
        logging.error(f"An error occurred during the test run: {e}", exc_info=True)
    finally:
        if cache_name:
            utils.delete_cache(cache_name, api_key)
        logging.info(f"--- SINGLE TEST (Pass {pass_number}, Group: {group_id}) COMPLETE ---")

if __name__ == '__main__':
    # --- CONFIGURE YOUR TEST HERE ---
    TEST_PASS_NUMBER = 1      # Set to 1 or 2
    TEST_GROUP_ID = "grp_1267"  # Set to the group_id you want to test (e.g., from community_samples.txt)
    # --------------------------------

    load_dotenv()
    google_api_key = os.getenv("GOOGLE_API_KEY")
    if not google_api_key:
        logging.fatal("FATAL: GOOGLE_API_KEY environment variable not found.")
    else:
        run_test(api_key=google_api_key, pass_number=TEST_PASS_NUMBER, group_id=TEST_GROUP_ID)
