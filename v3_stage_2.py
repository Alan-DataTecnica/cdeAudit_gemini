# v2_stage_2_grouping.py (v4 - Advanced Stats & Tuning)

import os
import json
import logging
import pandas as pd
import numpy as np
import networkx as nx
import faiss
from sentence_transformers import SentenceTransformer
import community.community_louvain as community_louvain
import torch
from tqdm import tqdm
import pickle
import sqlite3
import random
# --- NEW: Import plotting libraries ---
import matplotlib.pyplot as plt
import seaborn as sns


# --- 1. CONFIGURATION ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# -- Database Connection --
DATABASE_PATH = "cdeCatalogs/20250603_2030_cde.sqlite"
TABLE_NAME = "CDE_Dictionary_Condensed"
OUTPUT_DIR = os.path.join('outputs', 'stage_2')

# -- Checkpoint file paths --
GRAPH_CHECKPOINT_FILENAME = 'similarity_graph.gpickle'
EMBEDDINGS_CHECKPOINT_FILENAME = 'embeddings.npy'

# --- MODIFIED: Hub-and-Spoke and Graph Tuning Parameters ---
TOP_K_NEIGHBORS = 20  # Reduced from 50 to create more distinct communities
MAX_SUB_GROUP_SIZE = 200 # The Hub + ~199 closest neighbors
MIN_ORPHAN_GROUP_SIZE = 100 # Minimum size for a group of orphans
MIN_HUB_SPOKE_GROUP_SIZE = 10 # Hubs must form a group of at least this size

# -- Output Filenames --
COMMUNITY_DEFINITIONS_FILENAME = 'community_definitions.json'
STATS_OUTPUT_FILENAME = 'community_stats.txt'
SAMPLES_OUTPUT_FILENAME = 'community_samples.txt'
# --- NEW: Filename for advanced analysis plots ---
STATS_FIGURE_FILENAME = 'community_analysis_plots.png'

# -- Model & Fields --
EMBEDDING_MODEL = 'cambridgeltl/SapBERT-from-PubMedBERT-fulltext'
SEMANTIC_FIELDS = [
    'title',
    'short_description',
    'preferred_question_text',
    'synonymous_terms',
    'alternate_titles',
]

# -- Graph Tuning Parameters --
LEXICAL_BOOST_FACTOR = 0.2
STRUCTURAL_BOOST_FACTOR = 0.15

# --- 2. HELPER FUNCTIONS (No changes from previous version) ---
def load_and_select_candidates(db_path: str, table_name: str) -> pd.DataFrame:
    """Connects to the source SQLite database, reads the CDE table, and returns a DataFrame."""
    logging.info(f"Connecting to source SQLite database at: {db_path}")
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database file not found: {db_path}")
    try:
        conn = sqlite3.connect(db_path)
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, conn)
        logging.info(f"Successfully loaded {len(df):,} rows from the database.")
        if 'ID' not in df.columns: raise ValueError("The database table must have an 'ID' column.")
        df['ID'] = pd.to_numeric(df['ID'], errors='coerce').astype('Int64').astype(str)
        df.dropna(subset=['ID'], inplace=True)
        for col in SEMANTIC_FIELDS + ['variable_name', 'permissible_values']:
            if col in df.columns:
                df[col] = df[col].astype(str).fillna('')
        logging.info(f"Data loading and preparation complete. {len(df)} valid rows selected.")
        return df
    finally:
        if conn: conn.close()

def generate_embeddings(df: pd.DataFrame, fields: list, model_name: str) -> np.ndarray:
    """Generates embeddings for CDEs using a SentenceTransformer model."""
    logging.info(f"Generating embeddings using model: {model_name}")
    device = "cuda" if torch.cuda.is_available() else "cpu"
    logging.info(f"Using device: {device}")
    model = SentenceTransformer(model_name, device=device)
    df['semantic_text'] = df[fields].fillna('').astype(str).agg(' '.join, axis=1)
    return model.encode(df['semantic_text'].tolist(), batch_size=128, show_progress_bar=True)

def jaccard_similarity(str1, str2):
    if not isinstance(str1, str) or not isinstance(str2, str): return 0.0
    a = set(str1.lower().split('_'))
    b = set(str2.lower().split('_'))
    intersection = len(a.intersection(b))
    union = len(a.union(b))
    return intersection / union if union != 0 else 0.0

def build_similarity_graph(df: pd.DataFrame, embeddings: np.ndarray) -> nx.Graph:
    """Builds a multi-faceted similarity graph."""
    logging.info("Building similarity graph...")
    faiss.normalize_L2(embeddings)
    index = faiss.IndexIDMap(faiss.IndexFlatIP(embeddings.shape[1]))
    cde_ids_int64 = df['ID'].astype(np.int64).values
    index.add_with_ids(embeddings, cde_ids_int64)
    G = nx.Graph()
    G.add_nodes_from(df['ID'].values)
    df_lookup = df.set_index('ID')
    for i in tqdm(range(len(embeddings)), desc="Building Graph Edges"):
        cde_id, row = df.iloc[i]['ID'], df.iloc[i]
        distances, neighbor_ids_int = index.search(np.array([embeddings[i]]), TOP_K_NEIGHBORS)
        for j, neighbor_id_int in enumerate(neighbor_ids_int[0]):
            if neighbor_id_int == -1: continue
            neighbor_id = str(neighbor_id_int)
            if cde_id == neighbor_id or G.has_edge(cde_id, neighbor_id): continue
            try:
                neighbor_row = df_lookup.loc[neighbor_id]
            except KeyError: continue
            semantic_score = max(0, distances[0][j])
            lexical_score = jaccard_similarity(row.get('variable_name'), neighbor_row.get('variable_name'))
            structural_score = 1.0 if row.get('value_format') == neighbor_row.get('value_format') else 0.0
            combined_weight = semantic_score * (1 + (LEXICAL_BOOST_FACTOR * lexical_score) + (STRUCTURAL_BOOST_FACTOR * structural_score))
            G.add_edge(cde_id, neighbor_id, weight=combined_weight)
    logging.info(f"Graph built with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges.")
    return G

# --- 3. CORE GROUPING LOGIC ---

def detect_and_format_communities_hub_spoke(G: nx.Graph) -> list:
    """
    Detects communities using Louvain and then applies the Hub-and-Spoke
    subdivision algorithm with MODIFIED logic for minimum group size.
    """
    logging.info("Detecting parent communities with Louvain algorithm...")
    partition = community_louvain.best_partition(G, weight='weight')
    logging.info(f"Found {len(set(partition.values()))} parent communities.")

    parent_communities = {}
    for node, community_id in partition.items():
        parent_communities.setdefault(community_id, []).append(node)

    output_structure = []
    group_counter = 0
    
    for cid, members in tqdm(parent_communities.items(), desc="Processing Parent Communities"):
        if not members: continue
        
        sub_groups = []
        community_subgraph = G.subgraph(members).copy()
        nodes_to_process = set(members)
        
        # --- MODIFIED: This loop now respects the minimum hub-spoke group size ---
        while len(nodes_to_process) >= MIN_ORPHAN_GROUP_SIZE:
            centrality = nx.degree_centrality(community_subgraph)
            if not centrality: break
            
            # Find the best available hub that can form a valid group
            hub_node, hub_spoke_group = None, []
            # Sort potential hubs by centrality to check the best ones first
            sorted_hubs = sorted(centrality, key=centrality.get, reverse=True)

            for potential_hub in sorted_hubs:
                neighbors = sorted(community_subgraph.adj[potential_hub].items(), key=lambda item: item[1]['weight'], reverse=True)
                spoke_nodes = [n for n, _ in neighbors[:MAX_SUB_GROUP_SIZE - 1]]
                potential_group = [potential_hub] + spoke_nodes
                
                # Check if the group is large enough
                if len(potential_group) >= MIN_HUB_SPOKE_GROUP_SIZE:
                    hub_node = potential_hub
                    hub_spoke_group = potential_group
                    break # Found a valid hub
            
            if not hub_node: # No node can form a large enough group
                break # Exit loop and treat all remaining as orphans

            sub_groups.append({
                "group_id": f"grp_{group_counter}", "group_type": "hub_and_spoke",
                "hub_cde_id": int(hub_node), "member_cde_ids": [int(node) for node in hub_spoke_group]
            })
            group_counter += 1

            nodes_processed = set(hub_spoke_group)
            nodes_to_process -= nodes_processed
            community_subgraph.remove_nodes_from(nodes_processed)

        # Handle all remaining nodes as orphans
        orphans = list(nodes_to_process)
        for i in range(0, len(orphans), MIN_ORPHAN_GROUP_SIZE):
            batch = orphans[i:i + MIN_ORPHAN_GROUP_SIZE]
            sub_groups.append({
                "group_id": f"grp_{group_counter}", "group_type": "orphan",
                "member_cde_ids": [int(node) for node in batch]
            })
            group_counter += 1
        
        if sub_groups:
            output_structure.append({
                "community_id": f"comm_{cid}", "total_cde_count": len(members),
                "member_cde_ids": [int(node) for node in members], "sub_groups": sub_groups
            })
            
    logging.info(f"Formatted {len(output_structure)} communities into {group_counter} total sub-groups.")
    return output_structure


# --- 4. OUTPUT & ANALYSIS FUNCTIONS ---

def save_output(data: list, dir_path: str, filename: str):
    """Saves data to a JSON file."""
    os.makedirs(dir_path, exist_ok=True)
    filepath = os.path.join(dir_path, filename)
    logging.info(f"Saving data to: {filepath}")
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)

def generate_basic_stats_and_samples(community_data: list, df: pd.DataFrame, dir_path: str):
    """Generates and saves basic stats and random samples of the community structures."""
    stats_path = os.path.join(dir_path, STATS_OUTPUT_FILENAME)
    samples_path = os.path.join(dir_path, SAMPLES_OUTPUT_FILENAME)
    
    # Basic Stats
    num_parent_communities = len(community_data)
    all_sub_groups = [sg for comm in community_data for sg in comm['sub_groups']]
    num_sub_groups = len(all_sub_groups)
    parent_sizes = [c['total_cde_count'] for c in community_data]
    sub_group_sizes = [len(sg['member_cde_ids']) for sg in all_sub_groups]
    
    with open(stats_path, 'w') as f:
        f.write("--- Stage 2: Community & Grouping Statistics ---\n\n")
        f.write(f"Total Parent Communities: {num_parent_communities}\n")
        f.write(f"Total Sub-Groups (Hub-Spoke + Orphan): {num_sub_groups}\n\n")
        if parent_sizes:
            f.write(f"Parent Community Size:\n  - Min: {min(parent_sizes)}\n  - Max: {max(parent_sizes)}\n  - Avg: {np.mean(parent_sizes):.2f}\n\n")
        if sub_group_sizes:
            f.write(f"Sub-Group Size:\n  - Min: {min(sub_group_sizes)}\n  - Max: {max(sub_group_sizes)}\n  - Avg: {np.mean(sub_group_sizes):.2f}\n")
    logging.info(f"Basic statistics file saved to: {stats_path}")

    # Random Samples
    df_lookup = df.set_index('ID')
    with open(samples_path, 'w') as f:
        f.write("--- Stage 2: Community Samples ---\n\n")
        sample_communities = random.sample(community_data, min(3, len(community_data)))
        for comm in sample_communities:
            f.write(f"{'='*53}\nPARENT COMMUNITY: {comm['community_id']} (Total CDEs: {comm['total_cde_count']})\n{'='*53}\n\n")
            hs_groups = [sg for sg in comm['sub_groups'] if sg['group_type'] == 'hub_and_spoke']
            if hs_groups:
                sample_hs_group = random.choice(hs_groups)
                hub_id = str(sample_hs_group['hub_cde_id'])
                f.write(f"--- Sample Hub-and-Spoke Group: {sample_hs_group['group_id']} ({len(sample_hs_group['member_cde_ids'])} members) ---\n")
                hub_info = df_lookup.loc[hub_id]
                f.write(f"  [HUB] ID: {hub_id} | Title: {hub_info['title']}\n")
                spokes = [m for m in sample_hs_group['member_cde_ids'] if str(m) != hub_id]
                for spoke_id in random.sample(spokes, min(5, len(spokes))):
                    f.write(f"    [Spoke] ID: {spoke_id} | Title: {df_lookup.loc[str(spoke_id), 'title']}\n")
                f.write("\n")
            orphan_groups = [sg for sg in comm['sub_groups'] if sg['group_type'] == 'orphan']
            if orphan_groups:
                sample_orphan_group = random.choice(orphan_groups)
                f.write(f"--- Sample Orphan Group: {sample_orphan_group['group_id']} ({len(sample_orphan_group['member_cde_ids'])} members) ---\n")
                members = sample_orphan_group['member_cde_ids']
                for member_id in random.sample(members, min(5, len(members))):
                    f.write(f"    [Orphan] ID: {member_id} | Title: {df_lookup.loc[str(member_id), 'title']}\n")
                f.write("\n")
    logging.info(f"Samples file saved to: {samples_path}")


# --- NEW: Function for Advanced Statistical Analysis and Visualization ---
def generate_advanced_community_stats(community_data: list, df: pd.DataFrame, dir_path: str):
    """
    Calculates advanced statistics about each community and generates plots.
    """
    logging.info("Generating advanced community statistics and plots...")
    
    stats_data = []
    # Define columns to check for emptiness
    columns_to_check = SEMANTIC_FIELDS + ['variable_name', 'permissible_values']

    df['semantic_text_len'] = df[SEMANTIC_FIELDS].fillna('').astype(str).agg(' '.join, axis=1).str.len()

    for comm in community_data:
        comm_id = comm['community_id']
        member_ids = [str(mid) for mid in comm['member_cde_ids']]
        comm_df = df[df['ID'].isin(member_ids)]

        if comm_df.empty: continue

        # Calculate average character length
        avg_len = comm_df['semantic_text_len'].mean()

        # Calculate emptiness percentage for each column
        emptiness = {f"{col}_empty_pct": (comm_df[col].fillna('').str.strip() == '').mean() * 100 for col in columns_to_check}
        
        stats_data.append({'community_id': comm_id, 'avg_char_len': avg_len, **emptiness})

    if not stats_data:
        logging.warning("No data available to generate advanced stats plots.")
        return
        
    stats_df = pd.DataFrame(stats_data)
    
    # --- Create Plots ---
    fig, axes = plt.subplots(2, 1, figsize=(15, 20), gridspec_kw={'hspace': 0.4})
    fig.suptitle('Advanced Community Analysis', fontsize=16)

    # 1. Boxplot of Average Character Lengths
    sns.boxplot(x=stats_df['avg_char_len'], ax=axes[0])
    axes[0].set_title('Distribution of Average CDE Text Length per Community')
    axes[0].set_xlabel('Average Character Length (Title + Descriptions)')

    # 2. Heatmap of Field Emptiness
    heatmap_data = stats_df.set_index('community_id')[[col for col in stats_df.columns if '_empty_pct' in col]]
    sns.heatmap(heatmap_data, ax=axes[1], cmap='viridis', annot=True, fmt=".0f", linewidths=.5)
    axes[1].set_title('Percentage of Empty Fields per Community (%)')
    axes[1].set_ylabel('Community ID')
    axes[1].set_xlabel('Metadata Field')
    
    # Save the figure
    figure_path = os.path.join(dir_path, STATS_FIGURE_FILENAME)
    plt.savefig(figure_path, bbox_inches='tight')
    plt.close(fig)
    logging.info(f"Advanced analysis plots saved to: {figure_path}")


# --- 5. MAIN EXECUTION ---
def main():
    """Main function to run the complete Stage 2 grouping process."""
    logging.info("--- Starting Stage 2: Intelligent CDE Grouping (v4) ---")
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    graph_checkpoint_path = os.path.join(OUTPUT_DIR, GRAPH_CHECKPOINT_FILENAME)
    embeddings_checkpoint_path = os.path.join(OUTPUT_DIR, EMBEDDINGS_CHECKPOINT_FILENAME)

    candidate_df = load_and_select_candidates(DATABASE_PATH, TABLE_NAME)
    if candidate_df.empty:
        logging.info("No candidate CDEs loaded. Exiting.")
        return

    if os.path.exists(graph_checkpoint_path):
        logging.info(f"Loading graph from checkpoint: {graph_checkpoint_path}")
        with open(graph_checkpoint_path, 'rb') as f: similarity_graph = pickle.load(f)
    else:
        # Generate or load embeddings
        if os.path.exists(embeddings_checkpoint_path) and len(np.load(embeddings_checkpoint_path)) == len(candidate_df):
            logging.info(f"Loading embeddings from checkpoint: {embeddings_checkpoint_path}")
            embeddings = np.load(embeddings_checkpoint_path)
        else:
            logging.info("No valid checkpoints found or size mismatch. Running full embedding process.")
            embeddings = generate_embeddings(candidate_df, SEMANTIC_FIELDS, EMBEDDING_MODEL)
            np.save(embeddings_checkpoint_path, embeddings)

        similarity_graph = build_similarity_graph(candidate_df, embeddings)
        logging.info(f"Saving graph checkpoint to: {graph_checkpoint_path}")
        with open(graph_checkpoint_path, 'wb') as f: pickle.dump(similarity_graph, f)

    community_definitions = detect_and_format_communities_hub_spoke(similarity_graph)
    
    if community_definitions:
        save_output(community_definitions, OUTPUT_DIR, COMMUNITY_DEFINITIONS_FILENAME)
        generate_basic_stats_and_samples(community_definitions, candidate_df, OUTPUT_DIR)
        # --- NEW: Call the advanced stats generation function ---
        generate_advanced_community_stats(community_definitions, candidate_df, OUTPUT_DIR)
    else:
        logging.warning("No communities were detected or formatted. Output files will be empty.")

    logging.info("--- Stage 2 complete. ---")
    
if __name__ == "__main__":
    main()