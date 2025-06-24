import json
print("Checking JSON format...")
with open('outputs/stage_2/similarity_communities.json', 'r') as f:
    data = json.load(f)
print(f"Successfully loaded {len(data)} communities.")
assert isinstance(data, list), "Output should be a list."
if data:
    assert isinstance(data[0], list), "Elements of the output should be lists."
    assert isinstance(data[0][0], int), "CDE IDs should be integers."
print("Internal structure is correct.")
import pandas as pd
# Get the number of unique CDEs that were fed into the script
candidates_df = pd.read_csv('outputs/stage_2/candidate_df.csv')
total_candidates = len(candidates_df)

# Get the number of unique CDEs present in the final communities
unique_ids_in_output = set(cde_id for community in data for cde_id in community)
total_unique_output_cdes = len(unique_ids_in_output)

print(f"Started with {total_candidates} candidate CDEs.")
print(f"Found {total_unique_output_cdes} unique CDEs in the final communities.")
assert total_candidates == total_unique_output_cdes, "Mismatch in CDE counts! Data was lost."
print("CDE count validated successfully. No data was lost.")
max_size = max(len(c) for c in data) if data else 0
print(f"Largest community size is: {max_size}")
assert max_size <= 25, "Found a community larger than MAX_COMMUNITY_SIZE!"
print("Hub-and-spoke subdivision size constraint validated.")
from collections import Counter
sizes = [len(c) for c in data]
size_distribution = Counter(sizes)
print("\nCommunity Size Distribution:")
for size, count in sorted(size_distribution.items()):
    print(f"  - Size {size}: {count} communities")