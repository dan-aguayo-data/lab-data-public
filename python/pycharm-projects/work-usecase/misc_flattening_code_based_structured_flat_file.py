import os

# Directory containing your original text files
input_directory = r'C:\Users\DanielAguayo\Downloads\btr-files-prod\btr-files-prod'

# Directory to save the processed files
output_directory = r'C:\Users\DanielAguayo\PycharmProjects\FunProjects\BTRflattened'

# Create the output directory if it doesn't exist
os.makedirs(output_directory, exist_ok=True)


def process_file(file_path, output_directory):
    # Read the content of the file
    with open(file_path, 'r') as file:
        lines = file.readlines()

    processed_lines = []
    current_line = ""

    for line in lines:
        line = line.rstrip('\n')  # Remove the newline character
        if line.startswith("88"):
            # Append the content (after 88,) to the current line
            current_line += line[3:].strip()
        else:
            # If there's a current line in progress, add it to processed_lines
            if current_line:
                processed_lines.append(current_line)
            current_line = line

    # Afer for loop ends. Add the last accumulated line
    if current_line:
        processed_lines.append(current_line)

    # Get the file name and create the new file path
    file_name = os.path.basename(file_path)
    new_file_path = os.path.join(output_directory, file_name.replace(".txt", "_processed.txt"))

    # Write the processed lines to a new file
    with open(new_file_path, 'w') as new_file:
        for line in processed_lines:
            new_file.write(line + '\n')

    print(f"Processed file saved as: {new_file_path}")


# Process all .txt files in the input directory
for filename in os.listdir(input_directory):
    if filename.endswith(".txt"):
        file_path = os.path.join(input_directory, filename)
        process_file(file_path, output_directory)
