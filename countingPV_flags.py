import pandas as pd

# Load the CSV file
df = pd.read_csv('outputs/stage_1/cde_catalog_processed.csv')

# Count how many times 'True' appears in 'flag_bad_permissibles'
flag_count = (df['flag_bad_permissibles'] == True).sum()

print(f"Number of 'True' flags in 'flag_bad_permissibles': {flag_count}")
