from pymongo import MongoClient

# connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["covid"]

# list of databases containing raw_data collections
databases = ['raw_data1', 'raw_data2', 'raw_data3','raw_data4', 'raw_data5', 'raw_data6','raw_data7', 'raw_data8', 'raw_data9','raw_data10', 'raw_data11', 'raw_data12',
             'raw_data13', 'raw_data14', 'raw_data15','raw_data16', 'raw_data17', 'raw_data18','raw_data19', 'raw_data20', 'raw_data21','raw_data22', 'raw_data23', 'raw_data24',
             'raw_data25', 'raw_data26', 'raw_data27','raw_data28', 'raw_data29', 'raw_data30','raw_data31', 'raw_data32']

# merge all raw_data collections into a new collection
db['raw_data_union'].drop()  # drop existing collection
for database in databases:
    db[database]['raw_data'].aggregate([
        { "$merge": { "into": "raw_data_union" } }
    ])
