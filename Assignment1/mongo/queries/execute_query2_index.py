from pymongo import MongoClient
import time

# connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["covid"]

# create index
db.raw_data1.create_index("raw_data.agebracket")

start_time = time.time()

# execute query and store results
results = db["raw_data1"].aggregate([
    {"$match": {"raw_data.agebracket": {"$exists": True}}},
    {"$unwind": "$raw_data"},
    {"$project": {
        "age_group": {
            "$bucket": {
                "groupBy": "$raw_data.agebracket",
                "boundaries": [0, 6, 11, 16, 21, 26, 31, 36, 41, 46, 51, 56, 61, 66, 71, 76, 81, 86, 91, 96, 101],
                "default": ">100",
                "output": {
                    "age_group": {
                        "$concat": [
                            {"$toString": {"$subtract": ["$$this", {"$mod": ["$$this", 5]}]}},
                            "-",
                            {"$toString": {"$subtract": [{"$add": ["$$this", 5]}, {"$mod": [{"$add": ["$$this", 5]}, 5]}]}}
                        ]
                    }
                }
            }
        }
    }},
    {"$group": {"_id": "$age_group", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 1}
])

end_time = time.time()
print(end_time - start_time)

# print results
for result in results:
    print(result)
