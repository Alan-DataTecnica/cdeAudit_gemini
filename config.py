# config.py
# Purpose: Centralizes all configuration settings, file paths, and constants
# for the application.

import os

# --- CONFIGURATION ---
CDE_CATALOG_PATH = os.path.join('outputs', 'stage_1', 'cde_catalog_processed.csv')
COMMUNITY_DEFS_PATH = os.path.join('outputs', 'stage_2', 'community_definitions.json')
SUGGESTIONS_DIR = os.path.join('outputs', 'stage_3', 'raw_responses')
STATE_FILE_PATH = os.path.join('outputs', 'stage_3', 'stateMGMT', 'review_progress.json')
FINAL_OUTPUT_DIR = os.path.join('outputs', 'stage_4')
ITEMS_PER_PAGE = 50 # For paginating global review views