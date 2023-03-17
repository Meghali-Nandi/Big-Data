import os

# Set the name of the database to import to
database_name = "covid"

# Get a list of all the JSON files in the current directory
json_files = [f for f in os.listdir('./data') if f.endswith('.json')]
# json_files = ["state_test_data.json"]

# Loop through each JSON file and import it into MongoDB
for json_file in json_files:
    # Get the name of the collection (which is the file name without the .json extension)
    collection_name = os.path.splitext(json_file)[0]
    print(json_file)

    # Construct the mongoimport command
    command = "mongoimport --db {} --collection {} --file ./{} --batchSize 1".format(database_name, collection_name, json_file)

    # Execute the command
    os.system(command)
