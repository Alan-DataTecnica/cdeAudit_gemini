### **Stage 2 Design & Implementation Plan: Intelligent CDE Grouping**

### **1\. Purpose & Core Principles**

The primary purpose of Stage 2 is to intelligently group Common Data Elements (CDEs) for review. We will transform the flat list of CDEs produced by Stage 1 into contextually rich **"communities."** This is the most critical step for maximizing the efficiency and intelligence of the entire pipeline.  
This stage directly serves our core principles:

* **Efficiency:** By only analyzing and grouping CDEs flagged in Stage 1, and by creating dense, relevant batches, we drastically reduce the total number of API calls required in Stage 3\. This saves significant time and money.  
* **Intelligence:** This stage moves beyond simple keyword searches. By creating a multi-faceted understanding of how CDEs relate to one another (semantically, lexically, and structurally), we empower the AI model in Stage 3 to perform much higher-level reasoning, such as true deduplication and harmonization of CDE families.  
* **Modularity:** This stage will be a single, self-contained Python script (stage\_2\_grouping.py). It takes one input file from Stage 1 and produces one output file for Stage 3\. It is a discrete, testable, and auditable component.

### **2\. Lessons Learned from Stage 1 & Moving Forward**

Our work on Stage 1 provided a crucial lesson that directly shapes the design of Stage 2: **Explicit, auditable methods are vastly superior to complex, inferred logic.**  
Our initial reliance on layered, brittle regular expressions was inefficient and error-prone. The breakthrough came when we adopted a dictionary-based mapping file. This made our assumptions clear, our logic simple, and the results auditable.  
We will carry this principle directly into Stage 2\. Instead of relying on a single, black-box similarity score, we will build a **knowledge graph** where our assumptions about what makes two CDEs "similar" are made explicit through multiple, tunable edge weights. This is the direct application of our most important lesson learned. My commitment is to deliver a complete, well-tested implementation of this design without omissions.

### **3\. The Implementation Strategy: Graph-Based Community Detection**

We will implement a three-step process within a single script to create our CDE communities.

#### **Step 3.1: Candidate Selection**

The script will not analyze the entire catalog. It will only consider CDEs that meet at least one of these criteria:

* The CDE has its needs\_audit flag set to True from Stage 1\.  
* The CDE's permissible\_values field was successfully standardized by the mapping file in Stage 1 (we want to group these to find harmonization opportunities).

This initial filtering ensures our resource-intensive analysis is focused only where it can provide value.

#### **Step 3.2: Semantic Embedding (The "Fingerprint")**

For each candidate CDE, we will create a "semantic fingerprint."

* **Process:** Concatenate the text from the most meaningful fields: title \+ short\_description \+ synonymous\_terms \+ preferred\_question\_text.  
* **Model:** Use a modern, efficient embedding model like text-embedding-004 to convert this concatenated string into a high-dimensional vector. This vector represents the CDE's semantic meaning.  
* **Result:** A vector for every candidate CDE.

#### **Step 3.3: Graph Construction & Community Detection**

This is the core of Stage 2's intelligence. We will not use simple clustering. We will build a graph to model the nuanced relationships between CDEs.

* **Nodes:** Each candidate CDE will be a node in the graph.  
* **Edges (The Key Innovation):** We will connect the nodes with edges that represent multiple facets of similarity. An edge between two CDEs will have several weights:  
  1. **Semantic Weight:** The cosine similarity between their two embedding vectors (from Step 3.2). This captures the overall meaning.  
  2. **Lexical Weight:** The Levenshtein distance or Jaccard similarity between their variable\_name fields. This is excellent for catching typos or minor variations.  
  3. **Structural Weight:** A score based on whether their value\_format and standardized permissible\_values formats are identical. This helps distinguish CDEs that are about the same topic but expect different types of answers (e.g., a "Yes/No" question vs. a numeric score).  
* **Community Detection:** Once the weighted graph is built, we will run a community detection algorithm like **Louvain** or **Leiden**. These algorithms excel at identifying naturally dense clusters of nodes.  
  * **Why this is better:** Unlike k-means, you don't need to specify the number of clusters. It finds the optimal number and size of "communities" based on how densely connected the nodes are. A group of five truly identical CDEs will form a small, extremely dense community. A larger "family" of related but distinct CDEs (like the blood pressure examples) will form a larger, less dense community.

### **4\. Practical Implementation Plan**

* **Script:** stage\_2\_grouping.py  
* **Inputs:** outputs/stage\_1/cde\_catalog\_processed.tsv  
* **Output:** outputs/stage\_2/similarity\_communities.json. This will be a single JSON file containing a list of lists, where each inner list is a community of CDE IDs to be processed together in Stage 3\.  
* **Key Libraries:** pandas, sentence-transformers, faiss-cpu (for efficient similarity search), networkx (for building the graph), python-louvain (for community detection).

#### **Conceptual Script Outline (stage\_2\_grouping.py)**

import pandas as pd  
from sentence\_transformers import SentenceTransformer  
import faiss  
import networkx as nx  
import community as community\_louvain \# from python-louvain

def main():  
    \# 1\. Load the processed data from Stage 1  
    df \= pd.read\_csv("outputs/stage\_1/cde\_catalog\_processed.tsv", sep='\\t')  
      
    \# 2\. Select candidate CDEs based on our criteria  
    candidate\_df \= select\_candidates\_for\_grouping(df)  
      
    \# 3\. Generate embeddings for the semantic fingerprint  
    embeddings \= generate\_cde\_embeddings(candidate\_df)  
      
    \# 4\. Build a multi-weighted graph  
    \#    \- Use FAISS to find nearest neighbors for efficient edge creation  
    \#    \- Add semantic, lexical, and structural edge weights  
    G \= build\_similarity\_graph(candidate\_df, embeddings)  
      
    \# 5\. Run community detection to find the final groups  
    partition \= community\_louvain.best\_partition(G, weight='combined\_weight')  
      
    \# 6\. Format and save the communities to a JSON file  
    save\_communities\_to\_json(partition, "outputs/stage\_2/similarity\_communities.json")

if \_\_name\_\_ \== "\_\_main\_\_":  
    main()  
