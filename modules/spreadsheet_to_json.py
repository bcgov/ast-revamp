import pandas as pd
import json

def clean_dataframe( df):
    """
    Cleans a DataFrame by resetting the index, removing empty rows,
    and standardizing missing values.

    Parameters:
        df (pd.DataFrame): The input DataFrame to be cleaned.

    Returns:
        pd.DataFrame: The cleaned DataFrame.
    """
    df = df.reset_index(drop=True)  # Reset index
    df.dropna(how='all', inplace=True)  # Drop rows where all values are NaN
    
    # Convert numeric columns with missing values to nullable integers
    for col in df.select_dtypes(include=["float", "int"]).columns:
        df[col] = df[col].astype('Int64')
    
    # Replace NaN with None
    df = df.where(pd.notna(df), None)
    
    # Optional: Replace None with 0 for specific columns (if needed)
    for col in df.select_dtypes(include=["Int64"]).columns:
        df[col] = df[col].fillna(0)
    
    return df

def create_spreadsheet_json(df):

    # Define a function to transform each row into the desired JSON structure
    def transform_row(row):
        global last_category

        # Capture the category
        if pd.notna(row.iloc[0]) and row.iloc[0] != "":
            last_category = row.iloc[0]  # Update category if it is specified

        # Collect summary_fields dynamically (assumes they start at column index 6)
        summary_fields = []
        for val in row.iloc[6:]:  # Adjust index to the last columns
            if pd.notna(val) and val != "":  # Only add non-empty, non-NaN fields
                summary_fields.append(val)

        # Build the JSON object dynamically
        return {
            "table_summary": {
                "category": last_category,
                "feature_name": row.iloc[1],
                "table": row.iloc[2],
                "query": row.iloc[3],
                "buffer": row.iloc[4],
                "label_field": row.iloc[5],
                "summary_fields": summary_fields
            }
        }

    # Initialize a variables to store the last valid category
    last_category = None

    # Apply the transformation to each row, ignoring None results
    json_data = [result for _, row in df.iterrows() if (result := transform_row(row)) is not None]

    # Save to a JSON file
    with open('output\output.json', 'w') as json_file:
        json.dump(json_data, json_file, indent=4)

    return json_file