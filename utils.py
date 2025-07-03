# utils.py
# Purpose: A collection of helper functions and business logic that can be
# reused across different parts of the application.

from typing import Dict, List

def get_status_index(status: str) -> int:
    """
    Converts a status string to its corresponding integer index.

    This is a simple UI helper. Streamlit's radio button widget can be controlled
    by an 'index' parameter. This function provides a clean way to map a
    human-readable status (like 'accepted') to the correct index (1),
    making the UI code more readable and less prone to errors from "magic numbers".
    """
    return ['pending', 'accepted', 'rejected', 'needs_pro_review'].index(status)

def sort_cdes_by_redundancy(cde_ids: List[str], suggestions: Dict) -> List[str]:
    """
    Sorts a list of CDE IDs so that redundant CDEs appear next to each other.

    This is a crucial User Experience (UX) function. Its goal is to group, or
    "cluster," related CDEs together in the review queue. This allows the human
    reviewer to see all members of a potential duplicate group at the same time,
    making it much easier to decide which CDE to keep and which to mark for deletion.

    How it works:
    1. It first iterates through the CDEs to identify all redundancy groups,
        storing them in a 'redundancy_map'.
    2. It then builds a new sorted list, ensuring that when it encounters the
        first member of a redundancy group, it adds ALL members of that group
        to the list at once.
    3. A 'processed' set is used to keep track of CDEs that have already been
        added to the new list, preventing duplicates.
    """
    redundancy_map, sorted_list, processed = {}, [], set()

    # Step 1: Identify all redundancy groups and map them.
    for cde_id in cde_ids:
        # Check if the suggestion for this CDE has the 'redundancy_flag'.
        if cde_suggs := suggestions.get(cde_id, {}):
            if cde_suggs.get("redundancy_flag"):
                # The group consists of the CDE itself plus any IDs it's
                # flagged as redundant with.
                group_members = {cde_id}
                redundant_ids_str = cde_suggs.get("redundant_with_ids")
                if redundant_ids_str and isinstance(redundant_ids_str, str):
                    group_members.update(redundant_ids_str.split('|'))
                group_members.discard('') #

                # A 'frozenset' is used as the dictionary key because sets are
                # mutable and cannot be keys, but frozensets are immutable.
                # This elegantly handles cases where CDE 'A' is redundant
                # with 'B', and 'B' is redundant with 'A'. Both pairs will
                # map to the same key.
                group_key = frozenset(group_members)
                if group_key not in redundancy_map:
                    redundancy_map[group_key] = list(group_members)

    # Step 2: Build the new sorted list, clustering the groups.
    for cde_id in cde_ids:
        if cde_id in processed:
            continue # Skip if we've already added this CDE as part of a group.
        
        is_in_group = False
        for group_members in redundancy_map.values():
            if cde_id in group_members:
                # This CDE is part of a redundancy group. Add all members of
                # this group (who are also in the current view) to our list.
                cluster_to_add = [mid for mid in group_members if mid in cde_ids and mid not in processed]
                sorted_list.extend(cluster_to_add)
                processed.update(cluster_to_add)
                is_in_group = True
                break # Move to the next CDE in the original list.

        if not is_in_group:
            # This CDE is not part of any redundancy group, so just append it.
            sorted_list.append(cde_id)
            processed.add(cde_id)
            
    return sorted_list