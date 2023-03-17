import os
import json

def remove_empty_keys(data):
    """
    Recursively removes all keys with blank or empty values from a dictionary.
    """
    if isinstance(data, dict):
        return {k: remove_empty_keys(v) for k, v in data.items() if v not in ['', None] and remove_empty_keys(v)}
    elif isinstance(data, list):
        return [remove_empty_keys(item) for item in data if item not in ['', None] and remove_empty_keys(item)]
    else:
        return data

def process_json_file(file_path):
    """
    Loads a JSON file, removes all keys with blank or empty values, and saves the result back to the same file.
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    data = remove_empty_keys(data)
    print(file_path)
    with open(file_path, 'w') as f:
        json.dump(data, f)

directory = '../api/'  # set the directory path
extension = '.json'  # set the file extension

# loop through all files in the directory
for file in os.listdir(directory):
    # check if the file has the correct extension
    if file.endswith(extension):
        # process the JSON file
        file_path = os.path.join(directory, file)
        process_json_file(file_path)
