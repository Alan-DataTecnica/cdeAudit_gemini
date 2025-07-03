# ui_components.py
# Purpose: Contains functions that render major, self-contained parts of the
# Streamlit user interface.

import streamlit as st
import pandas as pd
from typing import Dict, List, Any

# Import our other modules
import utils
import database

def display_cde_review_form(cde_ids_to_display: List[str], all_suggestions: Dict, cde_df_lookup: pd.DataFrame, form_key: str):
    """
    Displays the main review form for a list of CDEs.
    
    This function is responsible ONLY for rendering the Streamlit widgets.
    The logic for saving the data has been moved to the database.py module.
    """
    fields_to_ignore = {'quality_score', 'quality_review_flag', 'redundancy_flag', 'redundancy_action', 'redundant_with_ids'}

    def set_all_statuses(status: str):
        """Helper to set the status for all items on the page via session_state."""
        for cde_id in cde_ids_to_display:
            if not st.session_state.get(f"del_cb_{cde_id}", False):
                field_sugs = {k: v for k, v in all_suggestions.get(cde_id, {}).items() if k not in fields_to_ignore}
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
            if cde_id not in cde_df_lookup.index:
                st.warning(f"CDE ID `{cde_id}` found in suggestions but not in the main catalog. It may have been purged. Skipping.")
                continue

            cde_status_key = f"{cde_id}|__CDE_STATUS__"
            cde_status = st.session_state.review_state.get(cde_status_key, {}).get('status', 'active')
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
                    st.write("No actionable field suggestions for this CDE.")
                    continue

                table_data = [{"field": f, "original_value": str(cde_df_lookup.loc[cde_id, f]) if f in cde_df_lookup.columns else "", "suggested_value": str(s)} for f, s in field_sugs.items()]
                st.dataframe(pd.DataFrame(table_data), use_container_width=True, hide_index=True)
                st.markdown("**Review Actions:**")

                for field in field_sugs:
                    sug_key = f"{cde_id}|{field}"
                    status = st.session_state.review_state.get(sug_key, {}).get('status', 'pending')
                    
                    # Logic to handle the 'Accept All' checkbox for a single CDE
                    if st.session_state.get(f"accept_all_{cde_id}", False):
                        status_index = utils.get_status_index('accepted')
                    else:
                        status_index = utils.get_status_index(st.session_state.get(f"radio_{sug_key}", status))

                    st.radio(f"**`{field}`**:", options=['pending', 'accepted', 'rejected', 'needs_pro_review'], index=status_index, key=f"radio_{sug_key}", horizontal=True)

        # --- REFACTORED SAVE LOGIC ---
        if st.form_submit_button("Save All Decisions on this Page"):
            # This block is now much simpler. It gathers decisions from the UI
            # and passes them to the database module for saving.
            database.save_page_decisions(cde_ids_to_display, all_suggestions)
            st.success("Your decisions for this view have been saved!")
            st.rerun()