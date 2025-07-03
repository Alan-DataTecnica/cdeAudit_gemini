# database.py (Updated)
# Purpose: Handles all read/write operations for the application's review state.

import os
import json
from typing import Dict, Any

# Import the configuration module instead of hardcoding paths
import config

def get_review_state() -> Dict[str, Any]:
    """
    Loads the review state from the JSON file specified in the config.
    If the file doesn't exist, it returns an empty dictionary.
    """
    if os.path.exists(config.STATE_FILE_PATH):
        with open(config.STATE_FILE_PATH, 'r') as f:
            return json.load(f)
    return {}

def save_review_state(state_data: Dict[str, Any]):
    """
    Saves the entire review state dictionary to the JSON file specified in the config.
    Ensures the output directory exists.
    """
    os.makedirs(os.path.dirname(config.STATE_FILE_PATH), exist_ok=True)
    with open(config.STATE_FILE_PATH, 'w') as f:
        json.dump(state_data, f, indent=2)