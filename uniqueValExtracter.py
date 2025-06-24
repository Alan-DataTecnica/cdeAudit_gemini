import pandas as pd

def extract_and_save_random_values(input_file, output_file, column_name, num_values=100):
    """
    Reads a column from a CSV file, extracts a specified number of unique random values,
    and saves them to a new CSV file.

    Args:
        input_file (str): The path to the source CSV file.
        output_file (str): The path where the output CSV file will be saved.
        column_name (str): The name of the column to read.
        num_values (int): The number of unique random values to extract.
    """
    try:
        # Read the source CSV file
        df = pd.read_csv(input_file)

        # Check if the specified column exists
        if column_name not in df.columns:
            print(f"Error: Column '{column_name}' not found in '{input_file}'.")
            return

        # Get unique, non-null values from the specified column
        unique_values = df[column_name].dropna().unique()

        # Check if there are enough unique values to sample from
        if len(unique_values) < num_values:
            print(f"Warning: Only {len(unique_values)} unique values found, which is less than {num_values}.")
            print(f"Exporting all {len(unique_values)} unique values.")
            random_sample = list(unique_values)
        else:
            # Randomly sample the specified number of unique values
            random_sample = pd.Series(unique_values).sample(n=num_values, random_state=1).tolist()

        # Create a new DataFrame for the output
        output_df = pd.DataFrame(random_sample, columns=['randomly_selected_values'])

        # Save the new DataFrame to a CSV file
        output_df.to_csv(output_file, index=False)
        
        print(f"Success! {len(random_sample)} random values have been saved to '{output_file}'.")

    except FileNotFoundError:
        print(f"Error: The file '{input_file}' was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # --- Configuration ---
    # 👈 Replace with the path to your source CSV file
    source_csv_file = 'cdeCatalogs/cdeCatalog.csv'      
    
    # 👈 The column you want to read from the source file
    target_column = 'permissible_values' 
    
    # 👈 The name of the file that will be created with the output
    output_csv_file = 'randomSelectOfPVs.csv' 
    
    number_to_extract = 100

    # --- Execution ---
    extract_and_save_random_values(source_csv_file, output_csv_file, target_column, number_to_extract)