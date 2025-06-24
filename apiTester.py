#!/usr/bin/env python3
import os
import json
import time
import logging
import requests
from typing import Dict, Any


# Instead of a relative name, build a full path in your script folder:
SCRIPT_DIR      = os.path.dirname(os.path.abspath(__file__))
ERROR_LOG_PATH  = os.path.join(SCRIPT_DIR, "stage3_error_log.txt")
TOKEN_LOG_PATH  = os.path.join(SCRIPT_DIR, "stage3_token_log.csv")
LOG_FILE        = os.path.join(SCRIPT_DIR, "stage3_pipeline.log")

# ── CONFIG ──
API_KEY             = "AIzaSyA3qCiyLqqCuzq4rrhY6OJPOR5bgc7exDk"
if not API_KEY:
    raise RuntimeError("Please set GOOGLE_API_KEY and re-run.")

BASE_URL            = "https://generativelanguage.googleapis.com/v1beta"
MODEL_NAME          = "models/gemini-2.5-flash-preview-05-20"   # drop any leading "models/"
CACHE_DISPLAY_NAME  = "cde_stage3_test_cache"
CACHED_SYSTEM_INSTRUCTION  = """
You are a precise healthcare data standardization assistant.
Return a flat JSON array of suggestions for the provided CDE group.
"""

INPUT_GROUPS_PATH   = "outputs/stage_2/similarity_communities.json"
ERROR_LOG_PATH      = "stage3_error_log.txt"

# ── SETUP LOGGING ──
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

def log_error(group_id: str, err: Exception, details: dict):
    with open(ERROR_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(f"--- ERROR: group {group_id} @ {time.ctime()} ---\n")
        f.write(f"Error Type: {type(err).__name__}\n")
        f.write(f"Error Message: {err}\n")
        for k, v in details.items():
            f.write(f"{k}: {v}\n")
        f.write("---------------\n\n")
    # also mirror to console
    print(f"\n[Logged error for group {group_id} in {ERROR_LOG_PATH}]\n")

def create_cache_via_rest(api_key: str) -> Optional[str]:
    url = f"{BASE_URL}/cachedContents?key={api_key}"
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

def generate_content_via_rest(
    prompt_text: str,
    cache_name: str,
    group_id: str
) -> Dict[str, Any]:
    url = f"{BASE_URL}/{MODEL_NAME}:generateContent?key={API_KEY}"
    body = {
        "cachedContent": cache_name,
        "contents": [{"role":"user","parts":[{"text":prompt_text}]}],
        "generationConfig": {
            "maxOutputTokens": 1200,
            "temperature": 0.3,
            "responseMimeType": "application/json"
        }
    }
    resp = requests.post(url, json=body, headers={"Content-Type":"application/json"}, timeout=420)
    if not resp.ok:
        details = {
            "request_url": url,
            "status_code": resp.status_code,
            "response_text": resp.text,
            "request_payload": json.dumps(body, indent=2)
        }
        log_error(group_id, RuntimeError("REST API failure"), details)
        raise RuntimeError(f"REST API call failed: {resp.status_code}")
    return resp.json()

def main():
    # load just the first group for a quick test
    with open(INPUT_GROUPS_PATH, "r", encoding="utf-8") as f:
        groups = json.load(f)
    if not groups:
        logging.error("No groups found in input.")
        return
    # Stage 2 output is a list of ID lists, e.g. [[id1,id2,…],[…],…]
    group_list = groups[0]
    # Use the first CDE ID in that list as the group_id
    group_id = str(group_list[0] if group_list else "test")

    # 1) Create or reuse cache
    cache_name = create_cache()

    # 2) Build prompt (mimics your stage3 format)
    prompt_payload = {"cde_group_for_review": group["CDEs"]}
    prompt_text = json.dumps(prompt_payload)

    # 3) Call the API
    logging.info(f"Generating content for group {group_id} …")
    result = generate_content_via_rest(prompt_text, cache_name, group_id)

    # 4) Output everything so you can inspect
    print("\n=== FULL API RESPONSE ===")
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    main()
