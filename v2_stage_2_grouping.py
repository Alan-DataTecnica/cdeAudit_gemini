# stage_2_grouping.py (v3 - with consultant fixes)

import os
import json
import csv
import logging
import pandas as pd
import numpy as np
import networkx as nx
import faiss
from sentence_transformers import SentenceTransformer
import community.community_louvain as community_louvain # FIX: More specific import
import torch
from transformers import AutoTokenizer, AutoModel
from tqdm import tqdm
import pickle
import sqlite3

# --- 1. CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# -- File Paths --
# -- Database Connection --
DATABASE_PATH = "cdeCatalogs/20250603_2030_cde.sqlite" # <-- IMPORTANT: Set this to your actual database file path
TABLE_NAME = "CDE_Dictionary_Condensed" # <-- IMPORTANT: Set this to the name of the table in your database
OUTPUT_DIR = os.path.join('outputs', 'stage_2')

# -- Checkpoint file paths --
GRAPH_CHECKPOINT_FILENAME = 'similarity_graph.gpickle'
EMBEDDINGS_CHECKPOINT_FILENAME = 'embeddings.npy'
CANDIDATES_CHECKPOINT_FILENAME = 'candidate_df.csv'

OUTPUT_FILENAME = 'similarity_communities.json'

# -- Model & Fields --
EMBEDDING_MODEL = 'cambridgeltl/SapBERT-from-PubMedBERT-fulltext'
SEMANTIC_FIELDS = [
    'title', 
    'short_description', 
    'preferred_question_text', 
    'synonymous_terms',
    'alternate_titles',
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
    'alternate_headers'
    ]

# -- Graph Tuning Parameters (Re-tuned for SapBERT) --
TOP_K_NEIGHBORS = 50  # This is likely still a good value to start.
LEXICAL_BOOST_FACTOR = 0.2 # Decreased from 0.5
STRUCTURAL_BOOST_FACTOR = 0.15 # Decreased from 0.3

# -- Community Handling --
MAX_BATCH_SIZE = 250

# --- 2. HELPER FUNCTIONS ---

def load_and_select_candidates(db_path: str, table_name: str) -> pd.DataFrame:
    """
    Connects directly to the source SQLite database, reads the entire CDE table,
    and returns a clean DataFrame ready for processing.
    """
    logging.info(f"Connecting to source SQLite database at: {db_path}")
    if not os.path.exists(db_path):
        logging.error(f"Database file not found at: {db_path}")
        raise FileNotFoundError(f"Database file not found: {db_path}")

    conn = None
    try:
        # Establish the database connection
        conn = sqlite3.connect(db_path)
        
        # Formulate the query to select all data from the table
        query = f"SELECT * FROM {table_name}"
        logging.info(f"Executing query: {query}")
        
        # Use pandas' robust SQL reader to load data directly into a DataFrame
        df = pd.read_sql_query(query, conn)
        logging.info(f"Successfully loaded {len(df):,} rows directly from the database.")

        # --- Perform Final Data Type Coercion ---
        # Ensure the essential 'ID' column is a string for all downstream processing
        if 'ID' not in df.columns:
            raise ValueError("The database table must have an 'ID' column.")
        
        df['ID'] = pd.to_numeric(df['ID'], errors='coerce').astype('Int64').astype(str)
        df.dropna(subset=['ID'], inplace=True)

        logging.info(f"Data loading and initial preparation complete. {len(df)} valid rows selected.")
        return df

    except Exception as e:
        logging.error(f"An error occurred while reading from the SQLite database: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

def generate_embeddings(df: pd.DataFrame, fields: list, model_name: str) -> np.ndarray:
    """
    Generates embeddings for CDEs using the SapBERT authors' recommended CLS-token approach.
    """
    logging.info(f"Generating embeddings using model: {model_name}")
    
    # Check for GPU
    device = "cuda" if torch.cuda.is_available() else "cpu"
    logging.info(f"Using device: {device}")

    # Load tokenizer and model
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModel.from_pretrained(model_name).to(device)
    
    # Create the text to be embedded
    df['semantic_text'] = df[fields].fillna('').astype(str).agg(' '.join, axis=1)
    all_names = df['semantic_text'].tolist()

    batch_size = 128 # Can be adjusted based on GPU memory
    all_embs = []

    logging.info("Encoding text to embeddings...")
    for i in tqdm(range(0, len(all_names), batch_size), desc="Embedding Batches"):
        batch_names = all_names[i:i + batch_size]
        
        # Tokenize the batch
        toks = tokenizer.batch_encode_plus(
            batch_names,
            padding="max_length",
            max_length=25, # As used in the SapBERT paper
            truncation=True,
            return_tensors="pt"
        )
        
        # Move tokens to the GPU
        toks_on_device = {k: v.to(device) for k, v in toks.items()}

        # Get the model output and extract the CLS token's representation
        with torch.no_grad():
            # The output is a tuple; the first element contains the hidden states
            cls_rep = model(**toks_on_device)[0][:, 0, :]
        
        all_embs.append(cls_rep.cpu().numpy())
    
    return np.concatenate(all_embs, axis=0)

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
            
            neighbor_row = df_lookup.loc[str(neighbor_id)]
            semantic_score = max(0, distances[0][j])
            lexical_score = jaccard_similarity(row['variable_name'], neighbor_row['variable_name'])
            structural_score = 1.0 if row['value_format'] == neighbor_row['value_format'] else 0.0
            combined_weight = semantic_score * (1 + (LEXICAL_BOOST_FACTOR * lexical_score) + (STRUCTURAL_BOOST_FACTOR * structural_score))
            G.add_edge(cde_id, neighbor_id, weight=combined_weight)
    logging.info(f"Graph built with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges.")
    return G

def detect_and_format_communities(G: nx.Graph) -> list:
    """
    Detects communities using Louvain and formats them into a structured
    list containing parent communities and their subdivided groups.
    """
    import time
    logging.info("Detecting parent communities with Louvain algorithm...")
    t0 = time.time()
    partition = community_louvain.best_partition(G, weight='weight')
    t1 = time.time()
    logging.info(f"Found {len(set(partition.values()))} parent communities in {t1-t0:.2f}s")

    # Group nodes by their parent community ID
    parent_communities: dict[int, list] = {}
    for node, community_id in partition.items():
        parent_communities.setdefault(community_id, []).append(node)

    # Build the final structured output
    output_structure = []
    group_counter = 0
    for cid, members in parent_communities.items():
        sub_groups = []
        # Subdivide the parent community into batches (groups)
        for i in range(0, len(members), MAX_BATCH_SIZE):
            batch = members[i : i + MAX_BATCH_SIZE]
            sub_groups.append({
                "group_id": f"grp_{group_counter}",
                "member_cde_ids": [int(node) for node in batch]
            })
            group_counter += 1

        output_structure.append({
            "community_id": f"comm_{cid}",
            "total_cde_count": len(members),
            "member_cde_ids": [int(node) for node in members],
            "sub_groups": sub_groups
        })
    
    logging.info(f"Formatted {len(output_structure)} parent communities into {group_counter} total subdivided groups.")
    return output_structure

# --- 4. SAVE COMMUNITIES AS EXPLICIT OBJECTS ---
def save_community_definitions(community_data: list, dir_path: str, filename: str):
    os.makedirs(dir_path, exist_ok=True)
    filepath = os.path.join(dir_path, filename)
    logging.info(f"Saving {len(community_data)} community definitions to: {filepath}")
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(community_data, f, indent=2)



# --- 3. MAIN EXECUTION ---
def main():
    """Main function to run the complete Stage 2 grouping process."""
    logging.info("--- Starting Stage 2: Intelligent CDE Grouping ---")
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Define checkpoint file paths
    graph_checkpoint_path = os.path.join(OUTPUT_DIR, GRAPH_CHECKPOINT_FILENAME)
    embeddings_checkpoint_path = os.path.join(OUTPUT_DIR, EMBEDDINGS_CHECKPOINT_FILENAME)

    # --- Step 1: Always load the full, clean dataset directly from the database ---
    # This is now the first step, ensuring we always start with the true source data.
    candidate_df = load_and_select_candidates(DATABASE_PATH, TABLE_NAME)
    
    if candidate_df.empty:
        logging.info("No candidate CDEs loaded from the database. Stage 2 complete.")
        # Save an empty community definition file
        save_community_definitions([], OUTPUT_DIR, "community_definitions.json")
        return

    # --- Step 2: Check for existing graph checkpoint ---
    if os.path.exists(graph_checkpoint_path):
        logging.info(f"Loading graph from pickle checkpoint: {graph_checkpoint_path}")
        with open(graph_checkpoint_path, 'rb') as f:
            similarity_graph = pickle.load(f)
        logging.info("Graph loaded successfully from checkpoint. Skipping embedding and graph building.")
    
    else:
        # --- Step 3: Check for embeddings checkpoint (if graph doesn't exist) ---
        if os.path.exists(embeddings_checkpoint_path):
            logging.info(f"Loading embeddings from checkpoint: {embeddings_checkpoint_path}")
            embeddings = np.load(embeddings_checkpoint_path)
            # Ensure the number of embeddings matches the number of CDEs loaded
            if len(embeddings) != len(candidate_df):
                 logging.warning("Embeddings checkpoint size does not match database row count. Regenerating embeddings.")
                 embeddings = generate_embeddings(candidate_df, SEMANTIC_FIELDS, EMBEDDING_MODEL)
                 logging.info(f"Saving new embeddings checkpoint to: {embeddings_checkpoint_path}")
                 np.save(embeddings_checkpoint_path, embeddings)
        else:
            # --- Step 4: Full process if no checkpoints exist ---
            logging.info("No valid checkpoints found. Running full embedding and graph process.")
            embeddings = generate_embeddings(candidate_df, SEMANTIC_FIELDS, EMBEDDING_MODEL)
            logging.info(f"Saving embeddings checkpoint to: {embeddings_checkpoint_path}")
            np.save(embeddings_checkpoint_path, embeddings)

        # Build the graph using the loaded or newly generated embeddings
        similarity_graph = build_similarity_graph(candidate_df, embeddings)
        
        logging.info(f"Saving graph checkpoint to pickle file: {graph_checkpoint_path}")
        with open(graph_checkpoint_path, 'wb') as f:
            pickle.dump(similarity_graph, f)

    # --- Step 5: Final community detection and saving ---
    community_definitions = detect_and_format_communities(similarity_graph)
    save_community_definitions(community_definitions, OUTPUT_DIR, "community_definitions.json")
        
    logging.info("--- Stage 2 complete. ---")
    
if __name__ == "__main__":
    main()