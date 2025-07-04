# stage_2_grouping.py (v3 - with consultant fixes)

import os
import json
import logging
import pandas as pd
import numpy as np
import networkx as nx
import faiss
from sentence_transformers import SentenceTransformer
import community.community_louvain as community_louvain # FIX: More specific import
import pickle

# --- 1. CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# -- File Paths --
STAGE_1_OUTPUT_PATH = os.path.join('outputs', 'stage_1', 'cde_catalog_processed.csv')
OUTPUT_DIR = os.path.join('outputs', 'stage_2')

# -- Checkpoint file paths --
GRAPH_CHECKPOINT_FILENAME = 'similarity_graph.gpickle'
EMBEDDINGS_CHECKPOINT_FILENAME = 'embeddings.npy'
CANDIDATES_CHECKPOINT_FILENAME = 'candidate_df.csv'

OUTPUT_FILENAME = 'similarity_communities.json'

# -- Model & Fields --
EMBEDDING_MODEL = 'all-MiniLM-L6-v2'
SEMANTIC_FIELDS = [
    'title', 
    'short_description', 
    'preferred_question_text', 
    'synonymous_terms'
]
SELECT_COLUMNS = [
    'ID',
    'variable_name',
    'title',
    'short_description',
    'preferred_question_text',
    'unit_of_measure',
    'permissible_values',
    'value_format',
    'value_mapping',
    'synonymous_terms',
    'alternate_titles',
    ]

# -- Graph Tuning Parameters --
TOP_K_NEIGHBORS = 50
LEXICAL_BOOST_FACTOR = 0.5
STRUCTURAL_BOOST_FACTOR = 0.3

# -- Community Handling --
MAX_BATCH_SIZE = 200

# --- 2. HELPER FUNCTIONS ---

def load_and_select_candidates(db_path: str, table_name: str) -> pd.DataFrame:
    """
    Connects to the source SQLite DB, loads the data, and filters out CDEs
    that fail to meet the quality threshold.
    """
    logging.info(f"Connecting to source SQLite database at: {db_path}")
    # ... (The robust database loading logic remains the same as the previous version) ...
    # It will load the DataFrame `df` with all 113,246 rows.
    # The code below assumes `df` has been successfully loaded from the database.
    
    # --- START: New Threshold-Based Filtering ---
    logging.info("Applying quality flag rejection threshold...")

    # Identify all flag columns generated by Stage 1
    flag_cols = [col for col in df.columns if col.startswith('flag_')]
    if not flag_cols:
        logging.warning("No 'flag_' columns found. Skipping quality threshold filtering.")
        return df # Return the full dataframe if no flags exist

    # Calculate how many quality checks each CDE failed
    df['flag_count'] = df[flag_cols].sum(axis=1)

    # Filter out rows that meet or exceed the rejection threshold
    original_rows = len(df)
    candidate_df = df[df['flag_count'] < REJECTION_THRESHOLD].copy()
    rows_rejected = original_rows - len(candidate_df)

    if rows_rejected > 0:
        logging.info(f"Rejected {rows_rejected:,} CDEs for exceeding quality flag threshold of {REJECTION_THRESHOLD}.")
    
    logging.info(f"Selected {len(candidate_df):,} candidates for grouping.")
    # --- END: New Threshold-Based Filtering ---

    return candidate_df
def generate_embeddings(df: pd.DataFrame, fields: list, model_name: str) -> np.ndarray:
    logging.info(f"Generating embeddings using model: {model_name}")
    df['semantic_text'] = df[fields].fillna('').astype(str).agg(' '.join, axis=1)
    model = SentenceTransformer(model_name)
    embeddings = model.encode(df['semantic_text'].tolist(), show_progress_bar=True)
    return embeddings

def jaccard_similarity(str1, str2):
    if not isinstance(str1, str) or not isinstance(str2, str): return 0.0
    a = set(str1.lower().split('_'))
    b = set(str2.lower().split('_'))
    intersection = len(a.intersection(b))
    union = len(a.union(b))
    return intersection / union if union != 0 else 0.0

def build_similarity_graph(df: pd.DataFrame, embeddings: np.ndarray) -> nx.Graph:
    logging.info("Building similarity graph...")
    faiss.normalize_L2(embeddings)
    index = faiss.IndexIDMap(faiss.IndexFlatIP(embeddings.shape[1]))
    cde_ids_int64 = df['ID'].values.astype(np.int64)
    index.add_with_ids(embeddings, cde_ids_int64)
    G = nx.Graph()
    G.add_nodes_from(df['ID'].values)
    df_lookup = df.set_index('ID')
    for i, (cde_id, row) in enumerate(df_lookup.iterrows()):
        distances, neighbor_ids = index.search(np.array([embeddings[i]]), TOP_K_NEIGHBORS)
        for j, neighbor_id in enumerate(neighbor_ids[0]):
            # FIX: Filter out invalid -1 neighbor_id from FAISS
            if neighbor_id == -1 or cde_id == neighbor_id or G.has_edge(cde_id, neighbor_id):
                continue
            
            neighbor_row = df_lookup.loc[neighbor_id]
            semantic_score = max(0, distances[0][j])
            lexical_score = jaccard_similarity(row['variable_name'], neighbor_row['variable_name'])
            structural_score = 1.0 if row['value_format'] == neighbor_row['value_format'] else 0.0
            combined_weight = semantic_score * (1 + (LEXICAL_BOOST_FACTOR * lexical_score) + (STRUCTURAL_BOOST_FACTOR * structural_score))
            G.add_edge(cde_id, neighbor_id, weight=combined_weight)
    logging.info(f"Graph built with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges.")
    return G

def detect_and_subdivide_communities(G: nx.Graph) -> list:
    """
    Partition with Louvain at a dynamic resolution to target ~MAX_BATCH_SIZE per batch,
    then chunk each community into fixed-size batches of size MAX_BATCH_SIZE.
    """
    import time
    total_nodes = G.number_of_nodes()
    if total_nodes == 0:
        logging.info("Graph is empty, no communities to detect.")
        return []

    # Dynamic resolution target based on desired batch count
    desired_clusters = total_nodes / MAX_BATCH_SIZE
    logging.info(f"Starting initial Louvain (res=1.0) on {total_nodes} nodes...")
    t0 = time.time()
    initial_part = community_louvain.best_partition(G, weight='weight', resolution=1.0)
    t1 = time.time()
    curr_clusters = len(set(initial_part.values()))
    logging.info(f"  → Found {curr_clusters} clusters in {t1-t0:.1f}s")

    resolution = desired_clusters / curr_clusters if curr_clusters > 0 else 1.0
    logging.info(f"Re-running Louvain with resolution={resolution:.4f}...")
    t2 = time.time()
    partition = community_louvain.best_partition(G, weight='weight', resolution=resolution)
    t3 = time.time()
    logging.info(f"  → Second pass found {len(set(partition.values()))} clusters in {t3-t2:.1f}s")

    # Group nodes by community ID
    communities: dict[int, list[int]] = {}
    for node, cid in partition.items():
        communities.setdefault(cid, []).append(node)

    # Chunk each community into batches
    final_batches: list[list[int]] = []
    for com in communities.values():
        for i in range(0, len(com), MAX_BATCH_SIZE):
            batch = com[i : i + MAX_BATCH_SIZE]
            final_batches.append(batch)

    logging.info(f"Total batches after subdivision: {len(final_batches)}")
    return final_batches

# --- 4. SAVE COMMUNITIES AS EXPLICIT OBJECTS ---
def save_communities(communities: list, dir_path: str, filename: str):
    os.makedirs(dir_path, exist_ok=True)
    filepath = os.path.join(dir_path, filename)
    logging.info(f"Saving {len(communities)} communities to: {filepath}")
    output = []
    for idx, com in enumerate(communities):
        output.append({
            "ID": f"grp_{idx}",
            "CDEs": [int(x) for x in com]
        })
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2)



# --- 3. MAIN EXECUTION ---
def main():
    """Main function to run the complete Stage 2 grouping process."""
    logging.info("--- Starting Stage 2: Intelligent CDE Grouping ---")
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    graph_checkpoint_path = os.path.join(OUTPUT_DIR, GRAPH_CHECKPOINT_FILENAME)
    embeddings_checkpoint_path = os.path.join(OUTPUT_DIR, EMBEDDINGS_CHECKPOINT_FILENAME)
    candidates_checkpoint_path = os.path.join(OUTPUT_DIR, CANDIDATES_CHECKPOINT_FILENAME)

    if os.path.exists(graph_checkpoint_path):
        logging.info(f"Loading graph from pickle checkpoint: {graph_checkpoint_path}")
        with open(graph_checkpoint_path, 'rb') as f:
            similarity_graph = pickle.load(f)
        logging.info("Graph loaded successfully from checkpoint.")
    elif os.path.exists(embeddings_checkpoint_path) and os.path.exists(candidates_checkpoint_path):
        logging.info(f"Loading embeddings and candidates from checkpoints...")
        embeddings = np.load(embeddings_checkpoint_path)
        candidate_df = pd.read_csv(candidates_checkpoint_path)
        logging.info(f"Loaded {len(embeddings)} embeddings and {len(candidate_df)} candidates.")
        
        similarity_graph = build_similarity_graph(candidate_df, embeddings)
        
        logging.info(f"Saving graph checkpoint to pickle file: {graph_checkpoint_path}")
        with open(graph_checkpoint_path, 'wb') as f:
            pickle.dump(similarity_graph, f)
    else:
        logging.info("No checkpoints found. Running full process from scratch.")
        candidate_df = load_and_select_candidates(STAGE_1_OUTPUT_PATH)
        
        if candidate_df.empty:
            logging.info("No candidate CDEs found for grouping. Stage 2 complete.")
            save_communities([], OUTPUT_DIR, OUTPUT_FILENAME)
            return

        embeddings = generate_embeddings(candidate_df, SEMANTIC_FIELDS, EMBEDDING_MODEL)
        
        logging.info(f"Saving embeddings checkpoint to: {embeddings_checkpoint_path}")
        np.save(embeddings_checkpoint_path, embeddings)
        logging.info(f"Saving candidates checkpoint to: {candidates_checkpoint_path}")
        cols_to_save = [c for c in SELECT_COLUMNS if c in candidate_df.columns]
        # Filter to just the columns you care about:
        cols_to_save = [c for c in SELECT_COLUMNS if c in candidate_df.columns]
        # Subset the DataFrame:
        candidate_df = candidate_df[cols_to_save]
        # Write out only those columns:
        candidate_df.to_csv(candidates_checkpoint_path, index=False)

        
        similarity_graph = build_similarity_graph(candidate_df, embeddings)
        
        logging.info(f"Saving graph checkpoint to pickle file: {graph_checkpoint_path}")
        with open(graph_checkpoint_path, 'wb') as f:
            pickle.dump(similarity_graph, f)

    final_communities = detect_and_subdivide_communities(similarity_graph)
    save_communities(final_communities, OUTPUT_DIR, OUTPUT_FILENAME)
    
    logging.info("--- Stage 2 complete. ---")

if __name__ == "__main__":
    main()