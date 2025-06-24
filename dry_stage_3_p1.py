# stage_3_test_harness.py
# Purpose: A dedicated script to isolate and test a single API call for either
# Pass 1 or Pass 2 of the Stage 3 adjudication process. It provides detailed
# debug output including the exact request, response, timing, and cost.
# Includes a DRY_RUN_MODE to estimate token consumption without making a full call.

import os
import json
import logging
import pandas as pd
from dotenv import load_dotenv
import time
from typing import Tuple, Optional

# --- Import from the shared utility module ---
import v2_shared_utils as utils
# --- Import prompts from the main scripts ---
from v4_stage_3_pass_1 import SYSTEM_PROMPT_PASS_1
from v4_stage_3_pass_2 import SYSTEM_PROMPT_PASS_2, AIResponsePass2, aggregate_and_filter_pass_1_results, create_pass_2_batches


def find_group_in_communities(group_id: str, community_definitions: list) -> Tuple[Optional[dict], Optional[dict]]:
    """Finds a specific group and its parent community from the definitions file."""
    for comm in community_definitions:
        for group in comm['sub_groups']:
            if group['group_id'] == group_id:
                return group, comm
    return None, None

def calculate_cost(prompt_tokens: int, output_tokens: int = 0) -> float:
    """Calculates the cost of an API call based on token counts."""
    cost = (prompt_tokens * utils.TOKEN_PRICING["input"]) + \
           (output_tokens * utils.TOKEN_PRICING["output"])
    return cost

def run_test(api_key: str, pass_number: int, group_id: str, dry_run: bool = False):
    """
    Main function to execute a single test run for a specified group and pass.
    """
    utils.setup_logging()
    logging.info(f"--- STARTING SINGLE TEST (Pass {pass_number}, Group: {group_id}) ---")
    if dry_run:
        logging.info("--- OPERATING IN DRY RUN MODE ---")
    logging.info(f"Using model specified in shared_utils.py: '{utils.MODEL_NAME}'")


    # --- 1. Load Common Files ---
    community_definitions_path = os.path.join('outputs', 'stage_2', 'community_definitions.json')
    processed_catalog_path = os.path.join('outputs', 'stage_1', 'cde_catalog_processed.csv')

    try:
        with open(community_definitions_path, 'r') as f:
            community_definitions = [utils.ParentCommunity.model_validate(item) for item in json.load(f)]
        cde_df = pd.read_csv(processed_catalog_path, dtype={'ID': str}, low_memory=False)
        cde_df['title'] = cde_df['title'].astype(str).fillna('')
        cde_lookup = cde_df.set_index('ID').to_dict('index')
    except Exception as e:
        logging.fatal(f"Could not load critical input files: {e}")
        return

    # --- 2. Prepare Payload ---
    prompt_payload = {}
    if pass_number == 1:
        target_group, _ = find_group_in_communities(group_id, [c.model_dump() for c in community_definitions])
        if not target_group:
            logging.error(f"Could not find group_id '{group_id}' in community definitions.")
            return
        
        cde_group_data = []
        for cde_id in target_group.get("member_cde_ids", []):
            full_cde = cde_lookup.get(str(cde_id))
            if full_cde:
                minimal_cde = {
                    "ID": full_cde.get('ID'), "title": full_cde.get('title'),
                    "short_description": full_cde.get('short_description'),
                    "variable_name": full_cde.get('variable_name'),
                    "permissible_values": full_cde.get('permissible_values'),
                    "value_mapping": full_cde.get('value_mapping'),
                    "quality_flags": {"is_bad_variable_name": full_cde.get('flag_bad_variable_name', False)}
                }
                cde_group_data.append(minimal_cde)
        prompt_payload = {"group_id_for_request": group_id, "cde_group_for_review": cde_group_data}
    elif pass_number == 2:
        cdes_to_process = aggregate_and_filter_pass_1_results(utils.RAW_DIR_PASS_1)
        pass_2_batches = create_pass_2_batches(cdes_to_process, [c.model_dump() for c in community_definitions], cde_lookup)
        target_batch = next((batch for batch in pass_2_batches if batch['group_id'] == group_id), None)
        if not target_batch:
            logging.error(f"Could not find or create a Pass 2 batch with group_id '{group_id}'.")
            return
        prompt_payload = {"group_id_for_request": group_id, "cde_group_for_review": target_batch["cde_data"]}
    else:
        logging.error("Invalid pass number specified. Must be 1 or 2.")
        return

    prompt_text_for_api = json.dumps(prompt_payload)

    # --- 3. Execute Dry Run or Full Run ---
    if dry_run:
        # --- DRY RUN LOGIC ---
        token_count_response = utils.count_tokens_via_rest(api_key, prompt_text_for_api)
        if token_count_response:
            prompt_tokens = token_count_response.get('totalTokens', 0)
            cost = calculate_cost(prompt_tokens)
            
            print("\n" + "="*80)
            print("--- DRY RUN: TOKEN ESTIMATE ---")
            print("NOTE: This count is for the main prompt payload only. It does not include the system instruction or cached context tokens, which are counted separately by the API in a real call.")
            print(f"Estimated Prompt Tokens: {prompt_tokens}")
            print(f"Estimated Minimum Cost (prompt only): ${cost:.8f} USD")
            print("="*80 + "\n")
        else:
            logging.error("Could not retrieve token count from the API.")
    else:
        # --- FULL RUN LOGIC ---
        community_context_text, system_prompt, cache_display_name, raw_response_dir = "", "", "", ""

        # Re-fetch context info needed for a full run
        if pass_number == 1:
            _, parent_community = find_group_in_communities(group_id, [c.model_dump() for c in community_definitions])
            community_member_titles = [str(cde_lookup.get(str(id), {}).get('title', '')) for id in parent_community['member_cde_ids']]
            community_context_text = "\n- ".join(filter(None, community_member_titles))
            system_prompt = SYSTEM_PROMPT_PASS_1
            cache_display_name = f"test_cache_{group_id}"
            raw_response_dir = utils.RAW_DIR_PASS_1
        elif pass_number == 2:
            pass_2_batches = create_pass_2_batches(aggregate_and_filter_pass_1_results(utils.RAW_DIR_PASS_1), [c.model_dump() for c in community_definitions], cde_lookup)
            target_batch = next((batch for batch in pass_2_batches if batch['group_id'] == group_id), None)
            parent_community_id = target_batch['community_id']
            community_context_map = {comm.community_id: "\n- ".join(filter(None, [str(cde_lookup.get(str(id), {}).get('title', '')) for id in comm.member_cde_ids])) for comm in community_definitions}
            community_context_text = community_context_map.get(parent_community_id, "")
            system_prompt = SYSTEM_PROMPT_PASS_2
            cache_display_name = f"test_cache_{group_id}"
            raw_response_dir = utils.RAW_DIR_PASS_2

        cache_name = None
        try:
            cache_name = utils.create_cache_via_rest(api_key, system_prompt, community_context_text, cache_display_name)
            if not cache_name:
                raise Exception("Test failed: Cache creation returned None.")

            final_request_body = {"cachedContent": cache_name, "contents": [{"role": "user", "parts": [{"text": prompt_text_for_api}]}], "generationConfig": {"temperature": 0.2, "responseMimeType": "application/json"}}
            print("\n" + "="*80 + "\n--- API REQUEST PAYLOAD ---\n" + json.dumps(final_request_body, indent=2) + "\n" + "="*80 + "\n")

            start_time = time.time()
            response_json = utils.generate_content_via_rest(prompt_text_for_api, cache_name, api_key, raw_response_dir)
            time_taken = end_time = time.time() - start_time

            print("\n" + "="*80 + "\n--- API RESPONSE ---\n" + json.dumps(response_json, indent=2) + "\n" + "="*80 + "\n")
            
            usage = response_json.get("usageMetadata", {})
            cost = calculate_cost(usage.get('promptTokenCount', 0), usage.get('candidatesTokenCount', 0))
            finish_reason = response_json.get("candidates", [{}])[0].get("finishReason", "N/A")

            print("\n" + "="*80 + "\n--- TEST SUMMARY ---\n" + f"Time Taken: {time_taken:.2f}s\nFinish Reason: {finish_reason}\nPrompt Tokens: {usage.get('promptTokenCount', 0)}\nOutput Tokens: {usage.get('candidatesTokenCount', 0)}\nTotal Tokens: {usage.get('totalTokenCount', 0)}\nEstimated Cost: ${cost:.8f} USD\n" + "="*80 + "\n")

        except Exception as e:
            logging.error(f"An error occurred during the test run: {e}", exc_info=True)
        finally:
            if cache_name: utils.delete_cache(cache_name, api_key)

    logging.info(f"--- SINGLE TEST (Pass {pass_number}, Group: {group_id}) COMPLETE ---")

if __name__ == '__main__':
    # --- CONFIGURE YOUR TEST HERE ---
    DRY_RUN_MODE = True      # Set to True to estimate tokens, False to make a real call.
    TEST_PASS_NUMBER = 1      # Set to 1 or 2
    TEST_GROUP_ID = "grp_1267"  # Set to the group_id you want to test
    # --------------------------------

    load_dotenv()
    google_api_key = os.getenv("GOOGLE_API_KEY")
    if not google_api_key:
        logging.fatal("FATAL: GOOGLE_API_KEY environment variable not found.")
    else:
        run_test(api_key=google_api_key, pass_number=TEST_PASS_NUMBER, group_id=TEST_GROUP_ID, dry_run=DRY_RUN_MODE)
