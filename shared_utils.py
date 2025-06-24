# shared_utils.py
# Purpose: A common utility module for Stage 3 processing.
# Contains shared configurations, Pydantic models, and core functions
# to be imported by both Pass 1 and Pass 2 scripts.

import os
import json
import time
import logging
import sys
import requests
from typing import List, Dict, Any, Optional

# Pydantic for validation
from pydantic import BaseModel, ValidationError, RootModel

# --- 1. CORE CONFIGURATION (Corrected) ---

# -- Directory and File Paths --
# Note: These paths assume the pass-specific scripts are run from the project root.
OUTPUT_DIR = "stage3_adjudication_output"
RAW_DIR_PASS_1 = os.path.join(OUTPUT_DIR, "pass_1_raw_responses")
RAW_DIR_PASS_2 = os.path.join(OUTPUT_DIR, "pass_2_raw_responses")
LOG_DIR = os.path.join(OUTPUT_DIR, "logs")

# -- Log and Manifest Filenames --
ERROR_LOG_FILENAME = "error_log.txt"
TOKEN_LOG_FILENAME = "token_log.csv"
PIPELINE_LOG_FILENAME = "pipeline.log"

# -- Worker and Safety Controls --
MAX_WORKERS = 8
MAX_CONSECUTIVE_ERRORS = 10
COST_LIMIT_USD = 250.0

# --- FIX: Restored the exact model name and pricing from the original working script ---
# -- API and Model Configuration (Corrected to match original script) --
MODEL_NAME = "models/gemini-2.5-pro"
BASE_API_URL = "https://generativelanguage.googleapis.com/v1beta"
CACHE_DISPLAY_NAME_PASS_1 = "cde_adjudication_cache_pass_1"
CACHE_DISPLAY_NAME_PASS_2 = "cde_adjudication_cache_pass_2"
TOKEN_PRICING = {
    "input": 0.00000125,
    "output": 0.00001000,
    "cached": 0.00000031
}


# --- 2. PYDANTIC MODELS FOR DATA VALIDATION ---

# Models for reading Stage 2 community definitions
class SubGroup(BaseModel):
    group_id: str
    member_cde_ids: List[int]
    group_type: str
    hub_cde_id: Optional[int] = None


class ParentCommunity(BaseModel):
    community_id: str
    total_cde_count: int
    member_cde_ids: List[int]
    sub_groups: List[SubGroup]


# Models for validating AI response in Pass 1
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


class AIResponsePass1(RootModel[List[AdjudicationResult]]):
    pass
    
# TODO: Add Pydantic models for Pass 2 response validation when the prompt is finalized.


# --- 3. HELPER FUNCTIONS: LOGGING, I/O, and API CALLS ---

def setup_logging():
    """Configures the root logger for the pipeline."""
    log_dir = os.path.join(OUTPUT_DIR, "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file_path = os.path.join(log_dir, PIPELINE_LOG_FILENAME)
    
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
        
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s",
        handlers=[
            logging.FileHandler(log_file_path, 'a'),
            logging.StreamHandler(sys.stdout)
        ]
    )

def log_error(group_id: str, err: Any, details: Dict[str, Any] = None):
    """Appends a formatted error message to the error log."""
    error_log_path = os.path.join(LOG_DIR, ERROR_LOG_FILENAME)
    error_message = f"--- ERROR: group {group_id} @ {time.ctime()} ---\n"
    error_message += f"Error Type: {type(err).__name__}\n"
    error_message += f"Error Message: {str(err)}\n"
    if details:
        error_message += f"--- Details ---\n{json.dumps(details, indent=2)}\n"
    error_message += "---------------\n\n"
    with open(error_log_path, 'a') as f:
        f.write(error_message)
    logging.error(f"Logged critical error for group {group_id}. See {error_log_path}.")


def log_token_usage(group_id: str, usage_metadata: Dict[str, int], pass_name: str):
    """Appends a token usage record to the CSV log."""
    token_log_path = os.path.join(LOG_DIR, TOKEN_LOG_FILENAME)
    header = "group_id,pass,prompt_tokens,cached_tokens,output_tokens,total_tokens,call_cost_usd\n"
    if not os.path.exists(token_log_path):
        with open(token_log_path, "w") as f:
            f.write(header)
            
    prompt_tokens = usage_metadata.get('promptTokenCount', 0)
    cached_tokens = usage_metadata.get('cachedContentTokenCount', 0)
    output_tokens = usage_metadata.get('candidatesTokenCount', 0)
    total_tokens = usage_metadata.get('totalTokenCount', 0)
    
    cost = (prompt_tokens * TOKEN_PRICING["input"]) + (output_tokens * TOKEN_PRICING["output"]) + (cached_tokens * TOKEN_PRICING["cached"])

    with open(token_log_path, "a") as f:
        f.write(f"{group_id},{pass_name},{prompt_tokens},{cached_tokens},{output_tokens},{total_tokens},{cost:.8f}\n")


def load_manifest(file_path: str) -> Dict[str, str]:
    """Loads a JSON manifest file if it exists."""
    if not os.path.exists(file_path):
        return {}
    with open(file_path, "r") as f:
        return json.load(f)


def save_manifest(file_path: str, manifest: Dict[str, str]):
    """Saves a manifest to a JSON file."""
    with open(file_path, "w") as f:
        json.dump(manifest, f, indent=2)


def create_cache_via_rest(api_key: str, system_prompt: str, community_context: str, display_name: str) -> Optional[str]:
    """Creates a short-lived cache for a community context."""
    url = f"{BASE_API_URL}/cachedContents?key={api_key}"
    body = {
        "model": MODEL_NAME,
        "displayName": display_name,
        "systemInstruction": {"parts": [{"text": system_prompt}]},
        "contents": [{"role": "user", "parts": [{"text": "### COMMUNITY CONTEXT ###\n" + community_context}]}],
        "ttl": "3600s"
    }
    logging.info(f"Attempting to create cache '{display_name}' via REST...")
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
    """Deletes a cache."""
    if not cache_name:
        return
    logging.info(f"Deleting cache: {cache_name}...")
    url = f"{BASE_API_URL}/{cache_name}?key={api_key}"
    try:
        requests.delete(url, timeout=15)
    except requests.exceptions.RequestException as e:
        log_error(f"CACHE_DELETE_{cache_name}", e)


def generate_content_via_rest(prompt_text: str, cache_name: str, api_key: str, raw_response_dir: str) -> dict:
    """Generates content using the REST API with a cached context."""
    url = f"{BASE_API_URL}/{MODEL_NAME}:generateContent?key={api_key}"
    body = {
        "cachedContent": cache_name,
        "contents": [{"role": "user", "parts": [{"text": prompt_text}]}],
        "generationConfig": {"temperature": 0.2, "responseMimeType": "application/json"}
    }
    
    resp = requests.post(url, headers={"Content-Type": "application/json"}, json=body, timeout=420)
    
    # Save raw response for auditing before checking status
    try:
        group_id_from_prompt = json.loads(prompt_text).get("group_id_for_request", f"unknown_{int(time.time())}")
        os.makedirs(raw_response_dir, exist_ok=True)
        raw_response_path = os.path.join(raw_response_dir, f"{group_id_from_prompt}_response.json")
        with open(raw_response_path, 'w', encoding='utf-8') as f:
            json.dump(resp.json(), f, indent=2)
    except Exception:
        # If response body isn't json, write raw text
        with open(raw_response_path, 'w', encoding='utf-8') as f:
            f.write(resp.text)
            
    resp.raise_for_status()
    return resp.json()
