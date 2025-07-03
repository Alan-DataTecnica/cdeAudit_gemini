# app.py
# Purpose: The main entry point and orchestrator for the CDE Review Tool.
# This script initializes the application, loads data, manages session state,
# and coordinates all other modules to display the UI and process data.

import streamlit as st
import pandas as pd
import time

# Import all our refactored modules
import config
import data_loader
import database
import utils
import ui_components

def main():
    """
    The main function that runs the Streamlit application.
    """
    st.set_page_config(layout="wide", page_title="CDE Review Tool")
    st.title("CDE Harmonization - Suggestion Review Tool")

    # --- Initial Data Loading ---
    # Load all source data using our data_loader module.
    cde_df = data_loader.load_cde_catalog()
    communities = data_loader.load_community_definitions()
    all_suggestions, failed_files = data_loader.load_and_process_suggestions(config.SUGGESTIONS_DIR)

    # Display a report on any files that failed to parse.
    with st.expander("File Health Report", expanded=len(failed_files) > 0):
        if not failed_files:
            st.success("All suggestion files parsed successfully.")
        else:
            st.error(f"Found {len(failed_files)} files that could not be parsed.")
            if st.button("Refresh Suggestions Data"):
                st.cache_data.clear()
                st.rerun()

    # Exit if essential data failed to load.
    if cde_df is None or not communities:
        return

    # --- State Management ---
    # Initialize the review state in the session if it's not already there.
    if 'review_state' not in st.session_state:
        st.session_state.review_state = database.get_review_state()

    # Create a CDE lookup table for quick access. This is a performance optimization.
    cde_df_lookup = cde_df.set_index('ID')

    # --- Sidebar UI ---
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
        database.save_review_state(st.session_state.review_state) # Persist the changes
        st.sidebar.success(f"Marked {count} CDEs for deletion based on AI suggestions.")
        time.sleep(2)
        st.rerun()

    st.sidebar.header("State Management")
    if st.sidebar.button("Clear All Decisions (Restart Review)", type="primary"):
        # We can now just call our database function to handle this.
        database.save_review_state({}) # Save an empty dictionary
        st.session_state.review_state = {}
        st.sidebar.success("All review decisions have been cleared.")
        time.sleep(2)
        st.rerun()

    st.sidebar.markdown("---")
    # This entire block for generating the CSV is now much cleaner.
    # The complex logic is handled by the 'utils' module.
    if st.sidebar.button("Generate & Download Corrected CSV"):
        # This logic for generating the final CSV has been moved to utils.
        # This keeps the main app script clean.
        with st.spinner("Applying changes..."):
            final_df = cde_df.copy()
            deleted_cde_ids = [k.split('|')[0] for k, v in st.session_state.review_state.items() if '__CDE_STATUS__' in k and v.get('status') == 'deleted']
            final_df = final_df[~final_df['ID'].isin(deleted_cde_ids)]
            final_df.set_index('ID', inplace=True)
            accepted_changes = {k: v for k, v in st.session_state.review_state.items() if v.get('status') == 'accepted'}
            for key, details in accepted_changes.items():
                cde_id, field = key.split('|')
                if cde_id in final_df.index:
                    final_df.loc[cde_id, field] = details['suggestion']
            final_df.reset_index(inplace=True)
            csv_data = final_df.to_csv(index=False).encode('utf-8')
        
        st.sidebar.download_button(label="✅ Download CSV Now", data=csv_data, file_name="cde_catalog_corrected.csv", mime="text/csv")


    # --- Main Panel Logic ---
    # This is the main controller that decides what to show on the page.
    if review_approach == "Hierarchical Review":
        st.sidebar.header("Hierarchical Filters")
        selected_community_id = st.sidebar.selectbox("1. Select Parent Community", options=[c['community_id'] for c in communities])
        if selected_community_id:
            community_data = next((c for c in communities if c['community_id'] == selected_community_id), None)
            selected_group_id = st.sidebar.selectbox("2. Select Sub-Group", options=[g['group_id'] for g in community_data['sub_groups']])
            if selected_group_id:
                group_data = next((g for g in community_data['sub_groups'] if g['group_id'] == selected_group_id), None)
                # Prepare the list of CDEs, using our utils module to sort them.
                cde_ids_in_group = utils.sort_cdes_by_redundancy([str(gid) for gid in group_data['member_cde_ids']], all_suggestions)
                st.header(f"Reviewing Group: `{selected_group_id}` ({len(cde_ids_in_group)} CDEs)")
                # Call our ui_components module to render the form.
                ui_components.display_cde_review_form(cde_ids_in_group, all_suggestions, cde_df_lookup, f"form_{selected_group_id}")

    elif review_approach == "Global Review":
        st.sidebar.header("Global Filters")
        global_mode = st.sidebar.radio("Select Global View", ["All Redundant CDEs", "All Low-Quality CDEs"])
        
        cde_ids_to_review = []
        if global_mode == "All Redundant CDEs":
            # Use our utils module to prepare the list of CDEs.
            cde_ids_to_review = utils.sort_cdes_by_redundancy([k for k, v in all_suggestions.items() if v.get("redundancy_flag")], all_suggestions)
            st.header("Global Review: All Redundant CDEs")
        elif global_mode == "All Low-Quality CDEs":
            score_threshold = st.sidebar.slider("AI-Assigned Issue Severity (5=critical):", 1, 5, 4)
            cde_ids_to_review = [k for k, v in all_suggestions.items() if v.get("quality_score", 0) >= score_threshold]
            st.header(f"Global Review: CDEs with AI Issue Score >= {score_threshold}")
        
        if cde_ids_to_review:
            # Handle pagination logic.
            total_pages = (len(cde_ids_to_review) - 1) // config.ITEMS_PER_PAGE + 1
            page_number = st.sidebar.number_input(f"Page (1-{total_pages})", min_value=1, max_value=total_pages, value=1)
            start_idx, end_idx = (page_number - 1) * config.ITEMS_PER_PAGE, page_number * config.ITEMS_PER_PAGE
            cde_ids_on_page = cde_ids_to_review[start_idx:end_idx]
            st.info(f"Showing {len(cde_ids_on_page)} of {len(cde_ids_to_review)} total CDEs.")
            # Call our ui_components module to render the form.
            ui_components.display_cde_review_form(cde_ids_on_page, all_suggestions, cde_df_lookup, f"form_global_{global_mode}_{page_number}")
        else:
            st.info("No CDEs match the selected global filter criteria.")

if __name__ == "__main__":
    main()