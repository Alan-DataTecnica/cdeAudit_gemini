# restore_review_state.py
# Purpose: A utility to recover from accidental deletions in the review tool.
# It parses the review state and AI suggestions, restoring any CDEs marked for
# deletion UNLESS the AI confidently suggested their deletion.

import os
import json
from tqdm import tqdm

# --- CONFIGURATION ---
SUGGESTIONS_DIR = os.path.join('outputs', 'stage_3', 'raw_responses') 
STATE_FILE_PATH = os.path.join('stage3_adjudication_output', 'review_progress.json')
# A CDE will NOT be restored if the AI suggested 'DELETE' AND gave this score.
AI_CONFIDENCE_SCORE_FOR_DELETION = 5

# --- DATA LOADING ---
def load_all_suggestions(suggestions_dir: str) -> dict:
    """Aggregates all raw suggestions from Pass 1 into a single dictionary."""
    all_suggestions = {}
    if not os.path.exists(suggestions_dir):
        print(f"Error: Suggestions directory not found at '{suggestions_dir}'.")
        return {}

    files_to_process = [f for f in os.listdir(suggestions_dir) if f.endswith(('.json', '.txt'))]
    print("Loading all AI suggestions...")
    for filename in tqdm(files_to_process, desc="Processing files"):
        filepath = os.path.join(suggestions_dir, filename)
        with open(filepath, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
                suggestions_list = []
                if isinstance(data, dict) and "candidates" in data:
                    text_content = data.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
                    if text_content: suggestions_list = json.loads(text_content)
                elif isinstance(data, list):
                    suggestions_list = data
                for sug in suggestions_list:
                    if cde_id := sug.get("ID"):
                        all_suggestions[str(cde_id)] = sug.get("suggestions", {})
            except Exception:
                # Silently skip malformed files in this utility
                continue
    return all_suggestions

def load_review_state() -> dict:
    """Loads the review progress from the state file."""
    if os.path.exists(STATE_FILE_PATH):
        with open(STATE_FILE_PATH, 'r') as f:
            return json.load(f)
    return {}

def save_review_state(state_data: dict):
    """Saves the review state back to the file."""
    with open(STATE_FILE_PATH, 'w') as f:
        json.dump(state_data, f, indent=2)

# --- CORE LOGIC ---
def restore_review_state():
    """
    Parses the review state and restores CDEs that were accidentally deleted.
    """
    print("--- Starting CDE Review State Restoration Utility ---")

    # 1. Load all necessary data
    review_state = load_review_state()
    all_suggestions = load_all_suggestions(SUGGESTIONS_DIR)

    if not review_state:
        print("Review state file is empty or not found. Nothing to restore.")
        return
    if not all_suggestions:
        print("Could not load any AI suggestions. Cannot perform restoration.")
        return

    # 2. Identify all CDEs currently marked for deletion
    deleted_cde_keys = [k for k, v in review_state.items() if '__CDE_STATUS__' in k and v.get('status') == 'deleted']
    
    if not deleted_cde_keys:
        print("No CDEs are currently marked for deletion. Nothing to do.")
        return
        
    print(f"Found {len(deleted_cde_keys)} CDE(s) marked for deletion. Analyzing...")

    # 3. Iterate and decide which to restore
    restored_count = 0
    skipped_count = 0
    keys_to_remove = []

    for key in deleted_cde_keys:
        cde_id = key.split('|')[0]
        ai_suggestion = all_suggestions.get(cde_id, {})
        
        # Check for the confident deletion condition
        is_confident_deletion = (
            ai_suggestion.get("redundancy_action") == "DELETE" and
            ai_suggestion.get("quality_score") == AI_CONFIDENCE_SCORE_FOR_DELETION
        )

        if is_confident_deletion:
            print(f"  - Skipping CDE {cde_id}: AI confidently suggested deletion.")
            skipped_count += 1
        else:
            print(f"  - Restoring CDE {cde_id}: Deletion was not confidently suggested by AI.")
            keys_to_remove.append(key)
            restored_count += 1
    
    # 4. Update the state file
    if keys_to_remove:
        for key in keys_to_remove:
            review_state.pop(key, None)
        save_review_state(review_state)
        print("\nSuccessfully updated the review state file.")
    else:
        print("\nNo CDEs met the criteria for restoration.")

    print("\n--- Restoration Summary ---")
    print(f"CDEs Restored: {restored_count}")
    print(f"CDEs Left Deleted (due to AI confidence): {skipped_count}")
    print("--- Restoration Utility Finished ---")


if __name__ == "__main__":
    restore_review_state()

