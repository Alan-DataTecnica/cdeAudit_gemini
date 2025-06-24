import pandas as pd
import json
import os

# Load the CSV file
input_file = 'cdeCatalogs/cdeCatalog.csv'
output_file = 'cdeCatalogs/cleanedPVs_cdeCat.csv'

# Check if file exists
if not os.path.isfile(input_file):
    raise FileNotFoundError(f"Input file '{input_file}' does not exist.")

# Read the CSV
df = pd.read_csv(input_file, dtype=str)

# Fill NaNs with empty strings to simplify checks
df.fillna('', inplace=True)

# Iterate through each row and process
for index, row in df.iterrows():
    pv = row['permissible_values']
    vm = row['value_mapping']

    if vm.strip() == '':
        try:
            parsed_json = json.loads(pv)
            if isinstance(parsed_json, dict):
                df.at[index, 'value_mapping'] = json.dumps(parsed_json)
                df.at[index, 'permissible_values'] = ''
        except (json.JSONDecodeError, TypeError):
            continue  # Leave the row unchanged if parsing fails

# Save the modified DataFrame to a new CSV
df.to_csv(output_file, index=False)

print(f"Updated CSV saved to '{output_file}'")
