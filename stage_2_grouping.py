# stage_2_grouping.py

import os
import json
import logging
import pandas as pd
import numpy as np
import networkx as nx
import faiss
from sentence_transformers import SentenceTransformer
import community as community_louvain

# --- 1. CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# -- File Paths --
# Assumes the script is run from the project's root directory
STAGE_1_OUTPUT_PATH = os.path.join('outputs', 'stage_1', 'cde_catalog_processed.csv')
OUTPUT_DIR = os.path.join('outputs', 'stage_2')
OUTPUT_FILENAME = 'similarity_communities.json'

# -- Model & Fields --
EMBEDDING_MODEL = 'all-MiniLM-L6-v2' # Efficient and effective for semantic similarity
SEMANTIC_FIELDS = [
    'title', 
    'short_description', 
    'preferred_question_text', 
    'synonymous_terms'
]

# -- Graph Tuning Parameters --
TOP_K_NEIGHBORS = 50  # Number of nearest neighbors to consider for building graph edges
LEXICAL_BOOST_FACTOR = 0.5 # How much a perfect lexical match boosts the semantic score
STRUCTURAL_BOOST_FACTOR = 0.3 # How much a perfect structural match boosts the score

# -- Community Handling --
MAX_COMMUNITY_SIZE = 25 # Threshold to trigger hub-and-spoke subdivision

# --- 2. HELPER FUNCTIONS ---

def load_and_select_candidates(filepath: str) -> pd.DataFrame:
    """
    Loads the processed CDE catalog from Stage 1 and selects candidates for grouping.

    Args:
        filepath: The path to the cde_catalog_processed.csv file.

    Returns:
        A pandas DataFrame containing only the CDEs that need grouping.
    """
    logging.info(f"Loading CDE catalog from: {filepath}")
    try:
        df = pd.read_csv(filepath)
        # Ensure boolean columns are treated as such, handling potential string values
        df['needs_audit'] = df['needs_audit'].astype(str).str.lower() == 'true'
        # This column will be added in the modified Stage 1 script
        if 'pv_was_standardized' not in df.columns:
            logging.warning("'pv_was_standardized' column not found. Assuming False.")
            df['pv_was_standardized'] = False
        else:
            df['pv_was_standardized'] = df['pv_was_standardized'].astype(str).str.lower() == 'true'

    except FileNotFoundError:
        logging.error(f"Fatal: Input file not found at {filepath}. Exiting.")
        raise

    # Candidate Selection Logic
    candidate_mask = (df['needs_audit'] == True) | (df['pv_was_standardized'] == True)
    candidate_df = df[candidate_mask].copy()
    
    logging.info(f"Selected {len(candidate_df)} candidates for grouping out of {len(df)} total CDEs.")
    return candidate_df

def generate_embeddings(df: pd.DataFrame, fields: list, model_name: str) -> np.ndarray:
    """
    Generates semantic embeddings for the candidate CDEs.

    Args:
        df: The DataFrame of candidate CDEs.
        fields: A list of text fields to concatenate for the embedding.
        model_name: The name of the SentenceTransformer model to use.

    Returns:
        A numpy array of embeddings.
    """
    logging.info(f"Generating embeddings using model: {model_name}")
    # Concatenate text fields, handling missing values gracefully
    df['semantic_text'] = df[fields].fillna('').astype(str).agg(' '.join, axis=1)
    
    model = SentenceTransformer(model_name)
    embeddings = model.encode(df['semantic_text'].tolist(), show_progress_bar=True)
    return embeddings

def jaccard_similarity(str1, str2):
    """Calculates Jaccard similarity between two strings (treated as sets of characters)."""
    if not isinstance(str1, str) or not isinstance(str2, str):
        return 0.0
    a = set(str1.lower().split('_'))
    b = set(str2.lower().split('_'))
    intersection = len(a.intersection(b))
    union = len(a.union(b))
    return intersection / union if union != 0 else 0.0

def build_similarity_graph(df: pd.DataFrame, embeddings: np.ndarray) -> nx.Graph:
    """
    Builds a multi-weighted graph based on semantic, lexical, and structural similarity.

    Args:
        df: The DataFrame of candidate CDEs.
        embeddings: The semantic embeddings for the CDEs.

    Returns:
        A networkx Graph with weighted edges.
    """
    logging.info("Building similarity graph...")
    # Normalize embeddings for cosine similarity calculation
    faiss.normalize_L2(embeddings)
    
    # Build a FAISS index for efficient similarity search
    index = faiss.IndexIDMap(faiss.IndexFlatIP(embeddings.shape[1]))
    index.add_with_ids(embeddings, df['ID'].values.astype(np.int64))

    G = nx.Graph()
    df_lookup = df.set_index('ID')

    # Iterate through each CDE to find its neighbors and create weighted edges
    for i, (cde_id, row) in enumerate(df_lookup.iterrows()):
        # Find Top K nearest neighbors using FAISS
        distances, neighbor_ids = index.search(np.array([embeddings[i]]), TOP_K_NEIGHBORS)
        
        for j, neighbor_id in enumerate(neighbor_ids[0]):
            if cde_id == neighbor_id or G.has_edge(cde_id, neighbor_id):
                continue

            neighbor_row = df_lookup.loc[neighbor_id]
            
            # 1. Semantic Score (from FAISS inner product)
            semantic_score = max(0, distances[0][j]) # Ensure non-negative

            # 2. Lexical Score (on variable_name)
            lexical_score = jaccard_similarity(row['variable_name'], neighbor_row['variable_name'])

            # 3. Structural Score (on data format)
            structural_score = 1.0 if row['value_format'] == neighbor_row['value_format'] else 0.0

            # 4. Combine scores with multiplicative boosting
            baseline = semantic_score
            lexical_boost = LEXICAL_BOOST_FACTOR * lexical_score
            structural_boost = STRUCTURAL_BOOST_FACTOR * structural_score
            combined_weight = baseline * (1 + lexical_boost + structural_boost)

            G.add_edge(cde_id, neighbor_id, weight=combined_weight)

    logging.info(f"Graph built with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges.")
    return G

def detect_and_subdivide_communities(G: nx.Graph) -> list:
    """
    Detects communities using the Louvain algorithm and subdivides oversized ones.

    Args:
        G: The similarity graph.

    Returns:
        A list of lists, where each inner list is a community of CDE IDs.
    """
    logging.info("Detecting communities with Louvain algorithm...")
    # Run community detection
    partition = community_louvain.best_partition(G, weight='weight')
    
    # Group nodes by community
    communities = {}
    for node, community_id in partition.items():
        if community_id not in communities:
            communities[community_id] = []
        communities[community_id].append(node)
    
    initial_communities = list(communities.values())
    logging.info(f"Detected {len(initial_communities)} initial communities.")
    
    # Post-processing: Hub-and-Spoke Subdivision
    logging.info(f"Subdividing communities larger than {MAX_COMMUNITY_SIZE}...")
    final_communities = []
    for community in initial_communities:
        if len(community) <= MAX_COMMUNITY_SIZE:
            final_communities.append(community)
        else:
            # Create a subgraph for the oversized community
            subgraph = G.subgraph(community)
            
            # Identify the "hub" CDE using weighted degree centrality
            centrality = nx.degree_centrality(subgraph)
            hub_node = max(centrality, key=centrality.get)
            
            # Get neighbors of the hub, sorted by edge weight
            neighbors = sorted(subgraph[hub_node].items(), key=lambda edge: edge[1]['weight'], reverse=True)
            neighbor_nodes = [n for n, _ in neighbors]
            
            # Create the "core" batch
            core_batch = [hub_node] + neighbor_nodes[:MAX_COMMUNITY_SIZE - 1]
            final_communities.append(core_batch)
            
            # Create the remaining "spoke" batches
            other_nodes = neighbor_nodes[MAX_COMMUNITY_SIZE - 1:]
            for spoke_node in other_nodes:
                final_communities.append([hub_node, spoke_node])

    logging.info(f"Final community count after subdivision: {len(final_communities)}.")
    return final_communities

def save_communities(communities: list, dir_path: str, filename: str):
    """
    Saves the final list of communities to a JSON file.

    Args:
        communities: The list of community lists.
        dir_path: The directory to save the file in.
        filename: The name of the output JSON file.
    """
    os.makedirs(dir_path, exist_ok=True)
    filepath = os.path.join(dir_path, filename)
    logging.info(f"Saving {len(communities)} communities to: {filepath}")
    
    # Convert all numpy integer types to standard Python int for JSON serialization
    final_communities_standard_int = [[int(cde_id) for cde_id in com] for com in communities]

    with open(filepath, 'w') as f:
        json.dump(final_communities_standard_int, f, indent=2)

# --- 3. MAIN EXECUTION ---

def main():
    """Main function to run the complete Stage 2 grouping process."""
    logging.info("--- Starting Stage 2: Intelligent CDE Grouping ---")
    
    # 1. Load and select candidate CDEs
    candidate_df = load_and_select_candidates(STAGE_1_OUTPUT_PATH)
    
    if candidate_df.empty:
        logging.info("No candidate CDEs found for grouping. Stage 2 complete.")
        # Save an empty list to the output file to signal completion
        save_communities([], OUTPUT_DIR, OUTPUT_FILENAME)
        return

    # 2. Generate embeddings for the semantic fingerprint
    embeddings = generate_embeddings(candidate_df, SEMANTIC_FIELDS, EMBEDDING_MODEL)
    
    # 3. Build a multi-weighted graph
    similarity_graph = build_similarity_graph(candidate_df, embeddings)
    
    # 4. Run community detection and subdivide large groups
    final_communities = detect_and_subdivide_communities(similarity_graph)
    
    # 5. Format and save the communities to a JSON file
    save_communities(final_communities, OUTPUT_DIR, OUTPUT_FILENAME)
    
    logging.info("--- Stage 2 complete. ---")

if __name__ == "__main__":
    main()