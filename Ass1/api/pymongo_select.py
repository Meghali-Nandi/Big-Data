import pymongo

# Create a MongoClient instance
client = pymongo.MongoClient()

# Select a database and collection
db = client['covid']
col = db['zones']

# Use the find() method to select all documents in the collection
cursor = col.find()

# Loop through the cursor to print the documents
for document in cursor:
    print(document)
