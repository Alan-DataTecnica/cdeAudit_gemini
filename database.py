# database.py (PostgreSQL Version)
# Purpose: Handles all read/write operations for the application's review state
# by connecting to a PostgreSQL database.

import streamlit as st
import psycopg2
from typing import Dict, Any, List

# --- Database Connection ---

def get_db_connection() -> psycopg2.extensions.connection | None:
    """
    Establishes a connection to the PostgreSQL database using credentials
    stored in Streamlit's secrets management.
    """
    try:
        conn = psycopg2.connect(st.secrets["DATABASE_URI"])
        return conn
    except Exception as e:
        st.error(f"Database connection error: {e}")
        return None

# --- Main Functions (Rewritten for SQL) ---

def get_review_state() -> Dict[str, Any]:
    """
    Loads the review state from the PostgreSQL database.

    This function queries the 'review_decisions' table and transforms the
    flat table data back into the nested dictionary format that the
    rest of the application expects.
    """
    conn = get_db_connection()
    if not conn:
        return {}

    review_state = {}
    with conn.cursor() as cur:
        cur.execute("SELECT cde_id, field, status, suggestion FROM review_decisions;")
        records = cur.fetchall()
        for record in records:
            cde_id, field, status, suggestion = record
            # Recreate the 'cde_id|field' key format
            key = f"{cde_id}|{field}"
            # Recreate the nested dictionary structure
            review_state[key] = {'status': status, 'suggestion': suggestion}

    conn.close()
    return review_state

def save_page_decisions(cde_ids_on_page: List[str], all_suggestions: Dict):
    """
    Processes and saves the decisions for all CDEs on the current page to the database.

    This function uses an "UPSERT" command (INSERT ON CONFLICT) to efficiently
    update existing decisions or insert new ones. It also handles deleting
    decisions that are changed back to 'pending'.
    """
    conn = get_db_connection()
    if not conn:
        st.error("Could not save decisions. Database connection failed.")
        return

    # For now, we'll use a placeholder for the user ID.
    # In a future step, you would get this from a login system.
    current_reviewer_id = "reviewer_1"
    
    fields_to_ignore = {'quality_score', 'quality_review_flag', 'redundancy_flag', 'redundancy_action', 'redundant_with_ids'}
    
    # The SQL command for inserting or updating a record.
    # ON CONFLICT(cde_id, field) tells Postgres what to do if a row with that
    # composite primary key already exists: it should UPDATE the fields instead
    # of trying to create a duplicate row.
    upsert_sql = """
        INSERT INTO review_decisions (cde_id, field, status, suggestion, reviewer_id, last_updated_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (cde_id, field) DO UPDATE SET
            status = EXCLUDED.status,
            suggestion = EXCLUDED.suggestion,
            reviewer_id = EXCLUDED.reviewer_id,
            last_updated_at = NOW();
    """
    
    delete_sql = "DELETE FROM review_decisions WHERE cde_id = %s AND field = %s;"

    with conn.cursor() as cur:
        for cde_id in cde_ids_on_page:
            # Handle CDE deletion status
            cde_status_key = f"{cde_id}|__CDE_STATUS__"
            field_for_db = "__CDE_STATUS__" # Special field name for CDE-level status

            if st.session_state.get(f"del_cb_{cde_id}"):
                cur.execute(upsert_sql, (cde_id, field_for_db, 'deleted', None, current_reviewer_id))
                continue
            else:
                # If the user *unchecks* delete, we remove the status from the DB.
                cur.execute(delete_sql, (cde_id, field_for_db))

            # Handle field-level suggestions
            accept_all_cde = st.session_state.get(f"accept_all_{cde_id}")
            field_sugs = {k:v for k,v in all_suggestions.get(cde_id, {}).items() if k not in fields_to_ignore}

            for field in field_sugs:
                new_status = st.session_state.get(f"radio_{cde_id}|{field}")
                if accept_all_cde:
                    new_status = 'accepted'

                if new_status and new_status != 'pending':
                    suggestion_text = field_sugs[field]
                    cur.execute(upsert_sql, (cde_id, field, new_status, suggestion_text, current_reviewer_id))
                else:
                    # If the status is 'pending', the decision should not be in the database.
                    cur.execute(delete_sql, (cde_id, field))
    
    # Commit the transaction to make all the changes permanent.
    conn.commit()
    conn.close()