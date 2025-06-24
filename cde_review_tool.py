# cde_review_tool.py
# Purpose: An interactive Streamlit application to review, manage, and
# approve AI-generated suggestions for the CDE catalog.

import streamlit as st
import pandas as pd
import os
import json
import time
from tqdm import tqdm
from typing import Dict, List, Any

# --- CONFIGURATION ---
CDE_CATALOG_PATH = os.path.join('outputs', 'stage_1', 'cde_catalog_processed.csv')
COMMUNITY_DEFS_PATH = os.path.join('outputs', 'stage_2', 'community_definitions.json')
SUGGESTIONS_DIR = os.path.join('outputs', 'stage_3', 'raw_responses') 
STATE_FILE_PATH = os.path.join('stage3_adjudication_output', 'review_progress.json')
FINAL_OUTPUT_DIR = os.path.join('outputs', 'stage_4')
ITEMS_PER_PAGE = 50 # For paginating global review views

# --- DATA LOADING & CACHING ---
@st.cache_data
def load_cde_catalog():
    """Loads the processed CDE catalog into a DataFrame."""
    if not os.path.exists(CDE_CATALOG_PATH):
        st.error(f"CDE Catalog not found. Expected at: {CDE_CATALOG_PATH}")
        return None
    return pd.read_csv(CDE_CATALOG_PATH, dtype={'ID': str}, low_memory=False)

@st.cache_data
def load_community_definitions():
    """Loads the community and group structure."""
    if not os.path.exists(COMMUNITY_DEFS_PATH):
        st.error(f"Community definitions not found. Expected at: {COMMUNITY_DEFS_PATH}")
        return []
    with open(COMMUNITY_DEFS_PATH, 'r') as f:
        return json.load(f)

@st.cache_data
def load_and_process_suggestions(suggestions_dir: str):
    """
    Aggregates all raw suggestions from Pass 1 into a structured format.
    Returns a tuple: (all_suggestions_dict, list_of_failed_files_with_errors)
    """
    all_suggestions, failed_files = {}, []
    if not os.path.exists(suggestions_dir):
        st.error(f"Suggestions directory not found. Expected at: {suggestions_dir}")
        return {}, []
    files_to_process = [f for f in os.listdir(suggestions_dir) if f.endswith(('.json', '.txt'))]
    for filename in tqdm(files_to_process, desc="Loading AI Suggestions"):
        filepath = os.path.join(suggestions_dir, filename)
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
            try:
                data, suggestions_list = json.loads(content), []
                if isinstance(data, dict) and "candidates" in data:
                    text_content = data.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
                    if text_content: suggestions_list = json.loads(text_content)
                elif isinstance(data, list):
                    suggestions_list = data
                for sug in suggestions_list:
                    if cde_id := sug.get("ID"):
                        all_suggestions[str(cde_id)] = sug.get("suggestions", {})
            except (json.JSONDecodeError, IndexError, TypeError) as e:
                failed_files.append({"file": filename, "error": str(e), "content": content})
    return all_suggestions, failed_files

# --- STATE MANAGEMENT ---
def load_review_state():
    if os.path.exists(STATE_FILE_PATH):
        with open(STATE_FILE_PATH, 'r') as f: return json.load(f)
    return {}

def save_review_state(state_data):
    os.makedirs(os.path.dirname(STATE_FILE_PATH), exist_ok=True)
    with open(STATE_FILE_PATH, 'w') as f: json.dump(state_data, f, indent=2)

# --- UI HELPER & LOGIC FUNCTIONS ---
def get_status_index(status: str) -> int:
    return ['pending', 'accepted', 'rejected', 'needs_pro_review'].index(status)

def sort_cdes_by_redundancy(cde_ids: List[str], suggestions: Dict) -> List[str]:
    redundancy_map, sorted_list, processed = {}, [], set()
    for cde_id in cde_ids:
        if cde_suggs := suggestions.get(cde_id, {}):
            if cde_suggs.get("redundancy_flag"):
                group_members = {cde_id}
                redundant_ids_str = cde_suggs.get("redundant_with_ids")
                if redundant_ids_str and isinstance(redundant_ids_str, str):
                    group_members.update(redundant_ids_str.split('|'))
                group_members.discard('')
                group_key = frozenset(group_members)
                if group_key not in redundancy_map: redundancy_map[group_key] = list(group_members)
    for cde_id in cde_ids:
        if cde_id in processed: continue
        is_in_group = False
        for group_members in redundancy_map.values():
            if cde_id in group_members:
                cluster_to_add = [mid for mid in group_members if mid in cde_ids and mid not in processed]
                sorted_list.extend(cluster_to_add); processed.update(cluster_to_add); is_in_group = True
                break
        if not is_in_group:
            sorted_list.append(cde_id); processed.add(cde_id)
    return sorted_list

def display_cde_review_form(cde_ids_to_display, all_suggestions, cde_df_lookup, form_key):
    """Displays the main review form for a list of CDEs."""
    fields_to_ignore = {'quality_score', 'quality_review_flag', 'redundancy_flag', 'redundancy_action', 'redundant_with_ids'}
    def set_all_statuses(status):
        for cde_id in cde_ids_to_display:
            if not st.session_state.get(f"del_cb_{cde_id}", False):
                field_sugs = {k:v for k,v in all_suggestions.get(cde_id, {}).items() if k not in fields_to_ignore}
                for field in field_sugs:
                    st.session_state[f"radio_{cde_id}|{field}"] = status
    st.markdown("---")
    st.subheader("Page-Level Actions")
    cols = st.columns(4)
    if cols[0].button("Set all to Pending", key=f"pending_all_top_{form_key}"): set_all_statuses('pending')
    if cols[1].button("✅ Accept All on Page", key=f"accept_all_top_{form_key}"): set_all_statuses('accepted')
    if cols[2].button("❌ Reject All on Page", key=f"reject_all_top_{form_key}"): set_all_statuses('rejected')
    if cols[3].button("📝 Flag All for Pro", key=f"pro_all_top_{form_key}"): set_all_statuses('needs_pro_review')
    st.markdown("---")
    with st.form(key=form_key):
        for cde_id in cde_ids_to_display:
            # --- FIX: Ensure the CDE exists in the main catalog before displaying ---
            if cde_id not in cde_df_lookup.index:
                st.warning(f"CDE ID `{cde_id}` found in suggestions but not in the main catalog. It may have been purged. Skipping.")
                continue

            cde_status_key, cde_status = f"{cde_id}|__CDE_STATUS__", st.session_state.review_state.get(f"{cde_id}|__CDE_STATUS__", {}).get('status', 'active')
            cde_sugs = all_suggestions.get(cde_id, {})
            title = f"CDE: {cde_id} - {cde_df_lookup.loc[cde_id].get('title', 'N/A')}"
            if (score := cde_sugs.get('quality_score')) is not None: title += f" | AI Score: {score}"
            if cde_sugs.get('redundancy_flag'): title += " | 🔗 Redundant Group"
            
            with st.expander(title, expanded=True):
                col1, col2 = st.columns([3, 1])
                is_del = col1.checkbox("Mark for Deletion", value=(cde_status == 'deleted'), key=f"del_cb_{cde_id}")
                col1.caption("This only marks the CDE. Deletion occurs upon final export.")
                col2.checkbox("✨ Accept All Suggestions", key=f"accept_all_{cde_id}")
                
                if is_del:
                    st.warning("This CDE will be deleted from the final export.")
                    continue
                
                field_sugs = {k: v for k, v in cde_sugs.items() if k not in fields_to_ignore}
                if not field_sugs:
                    st.write("No actionable field suggestions for this CDE."); continue
                
                table_data = [{"field": f, "original_value": str(cde_df_lookup.loc[cde_id, f]) if f in cde_df_lookup.columns else "", "suggested_value": str(s)} for f, s in field_sugs.items()]
                st.dataframe(pd.DataFrame(table_data), use_container_width=True, hide_index=True)
                st.markdown("**Review Actions:**")
                
                for field in field_sugs:
                    sug_key = f"{cde_id}|{field}"
                    status = st.session_state.review_state.get(sug_key, {}).get('status', 'pending')
                    
                    if st.session_state.get(f"accept_all_{cde_id}", False):
                        status_index = get_status_index('accepted')
                    else:
                        status_index = get_status_index(st.session_state.get(f"radio_{sug_key}", status))

                    st.radio(f"**`{field}`**:", options=['pending', 'accepted', 'rejected', 'needs_pro_review'], index=status_index, key=f"radio_{sug_key}", horizontal=True)

        if st.form_submit_button("Save All Decisions on this Page"):
            for cde_id in cde_ids_to_display:
                if cde_id not in cde_df_lookup.index: continue # Skip processing for purged CDEs
                
                cde_status_key = f"{cde_id}|__CDE_STATUS__"
                if st.session_state.get(f"del_cb_{cde_id}"):
                    st.session_state.review_state[cde_status_key] = {'status': 'deleted'}
                    continue
                elif cde_status_key in st.session_state.review_state:
                    st.session_state.review_state.pop(cde_status_key, None)
                
                accept_all_cde = st.session_state.get(f"accept_all_{cde_id}")
                field_sugs = {k:v for k,v in all_suggestions.get(cde_id, {}).items() if k not in fields_to_ignore}
                
                for field in field_sugs:
                    sug_key, new_status = f"{cde_id}|{field}", st.session_state.get(f"radio_{sug_key}")
                    if accept_all_cde: new_status = 'accepted'
                    if new_status and new_status != 'pending':
                        st.session_state.review_state[sug_key] = {'status': new_status, 'suggestion': field_sugs[field]}
                    elif sug_key in st.session_state.review_state:
                        st.session_state.review_state.pop(sug_key, None)
            
            save_review_state(st.session_state.review_state)
            st.success("Your decisions for this view have been saved!"); time.sleep(1); st.rerun()

# --- MAIN APPLICATION ---
def main():
    st.set_page_config(layout="wide", page_title="CDE Review Tool")
    st.title("CDE Harmonization - Suggestion Review Tool")

    cde_df, communities, (all_suggestions, failed_files) = load_cde_catalog(), load_community_definitions(), load_and_process_suggestions(SUGGESTIONS_DIR)

    with st.expander("File Health Report", expanded=len(failed_files) > 0):
        if not failed_files: st.success("All suggestion files parsed successfully.")
        else:
            st.error(f"Found {len(failed_files)} files that could not be parsed.")
            if st.button("Refresh Suggestions Data"): st.cache_data.clear(); st.rerun()

    if cde_df is None or not communities: return
    if 'review_state' not in st.session_state: st.session_state.review_state = load_review_state()
    cde_df_lookup = cde_df.set_index('ID')

    st.sidebar.header("Review Mode")
    review_approach = st.sidebar.radio("Select Review Approach", ["Hierarchical Review", "Global Review"])
    st.sidebar.markdown("---")
    
    st.sidebar.header("Automation Tools")
    if st.sidebar.button("Auto-apply AI Deletions"):
        count = 0
        for cde_id, suggs in all_suggestions.items():
            if suggs.get("redundancy_flag") and suggs.get("redundancy_action") == "DELETE":
                st.session_state.review_state[f"{cde_id}|__CDE_STATUS__"] = {'status': 'deleted'}
                count += 1
        save_review_state(st.session_state.review_state)
        st.sidebar.success(f"Marked {count} CDEs for deletion based on AI suggestions."); time.sleep(2); st.rerun()

    st.sidebar.header("State Management")
    if st.sidebar.button("Clear All Decisions (Restart Review)", type="primary"):
        if os.path.exists(STATE_FILE_PATH): os.remove(STATE_FILE_PATH)
        st.session_state.review_state = {}
        st.sidebar.success("All review decisions have been cleared."); time.sleep(2); st.rerun()
    
    st.sidebar.markdown("---")
    if st.sidebar.button("Generate & Download Corrected CSV"):
        with st.spinner("Applying changes..."):
            final_df = cde_df.copy()
            deleted_cde_ids = [k.split('|')[0] for k, v in st.session_state.review_state.items() if '__CDE_STATUS__' in k and v.get('status') == 'deleted']
            final_df = final_df[~final_df['ID'].isin(deleted_cde_ids)]
            final_df.set_index('ID', inplace=True)
            accepted_changes = {k: v for k, v in st.session_state.review_state.items() if v.get('status') == 'accepted'}
            for key, details in accepted_changes.items():
                cde_id, field = key.split('|')
                if cde_id in final_df.index: final_df.loc[cde_id, field] = details['suggestion']
            final_df.reset_index(inplace=True)
            csv_data = final_df.to_csv(index=False).encode('utf-8')
        st.sidebar.download_button(label="✅ Download CSV Now", data=csv_data, file_name="cde_catalog_corrected.csv", mime="text/csv")
    
    # Main panel logic follows...
    if review_approach == "Hierarchical Review":
        st.sidebar.header("Hierarchical Filters")
        selected_community_id = st.sidebar.selectbox("1. Select Parent Community", options=[c['community_id'] for c in communities])
        if selected_community_id:
            community_data = next((c for c in communities if c['community_id'] == selected_community_id), None)
            selected_group_id = st.sidebar.selectbox("2. Select Sub-Group", options=[g['group_id'] for g in community_data['sub_groups']])
            if selected_group_id:
                group_data = next((g for g in community_data['sub_groups'] if g['group_id'] == selected_group_id), None)
                cde_ids_in_group = sort_cdes_by_redundancy([str(gid) for gid in group_data['member_cde_ids']], all_suggestions)
                st.header(f"Reviewing Group: `{selected_group_id}` ({len(cde_ids_in_group)} CDEs)")
                display_cde_review_form(cde_ids_in_group, all_suggestions, cde_df_lookup, f"form_{selected_group_id}")

    elif review_approach == "Global Review":
        st.sidebar.header("Global Filters")
        global_mode = st.sidebar.radio("Select Global View", ["All Redundant CDEs", "All Low-Quality CDEs"])
        
        cde_ids_to_review = []
        if global_mode == "All Redundant CDEs":
            cde_ids_to_review = sort_cdes_by_redundancy([k for k, v in all_suggestions.items() if v.get("redundancy_flag")], all_suggestions)
            st.header("Global Review: All Redundant CDEs")
        elif global_mode == "All Low-Quality CDEs":
            score_threshold = st.sidebar.slider("AI-Assigned Issue Severity (5=critical):", 1, 5, 4)
            cde_ids_to_review = [k for k, v in all_suggestions.items() if v.get("quality_score", 0) >= score_threshold]
            st.header(f"Global Review: CDEs with AI Issue Score >= {score_threshold}")
        
        if cde_ids_to_review:
            total_pages = (len(cde_ids_to_review) - 1) // ITEMS_PER_PAGE + 1
            page_number = st.sidebar.number_input(f"Page (1-{total_pages})", min_value=1, max_value=total_pages, value=1)
            start_idx, end_idx = (page_number - 1) * ITEMS_PER_PAGE, page_number * ITEMS_PER_PAGE
            cde_ids_on_page = cde_ids_to_review[start_idx:end_idx]
            st.info(f"Showing {len(cde_ids_on_page)} of {len(cde_ids_to_review)} total CDEs.")
            display_cde_review_form(cde_ids_on_page, all_suggestions, cde_df_lookup, f"form_global_{global_mode}_{page_number}")
        else:
            st.info("No CDEs match the selected global filter criteria.")

if __name__ == "__main__":
    main()
