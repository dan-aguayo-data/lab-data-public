import pandas as pd
import re

# Function to convert CamelCase to UPPER_CASE_WITH_UNDERSCORES
def convert_to_upper_snake_case(name):
    # Step 1: Insert underscore before each capital letter followed by lowercase letters or numbers
    s1 = re.sub(r'(?<=[a-z0-9])([A-Z])', r'_\1', name)
    # Step 2: Insert underscores around numbers
    s2 = re.sub(r'([a-zA-Z])([0-9])', r'\1_\2', s1)
    s3 = re.sub(r'([0-9])([a-zA-Z])', r'\1_\2', s2)
    # Convert to upper case
    return s3.upper()

# Read the CSV file
input_file = r'C:\Users\DanielAguayo\PycharmProjects\KADA\PVO_naming.csv'
df = pd.read_csv(input_file)

# Apply the conversion function to the Original_Table_Name and Original_Column_Name columns
df['New_Table_Name'] = df['Original_Table_Name'].apply(convert_to_upper_snake_case)
df['New_Column_Name'] = df['Original_Column_Name'].apply(convert_to_upper_snake_case)

# Write the updated DataFrame to a new CSV file
output_file = r'C:\Users\DanielAguayo\PycharmProjects\KADA\PVO_naming_converted.csv'
df.to_csv(output_file, index=False)

print(f"Converted CSV saved to {output_file}")
