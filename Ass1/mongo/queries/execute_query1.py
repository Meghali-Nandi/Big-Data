from pymongo import MongoClient
import time
# connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["covid"]
start_time = time.time()
results = db.raw_data1.aggregate( [
    {"$match": {"raw_data.gender": {"$exists": True}}},
    {"$unwind": "$raw_data"},
    {"$group": {"_id": {"state": "$raw_data.detectedstate", "gender": "$raw_data.gender"}, 
                "count": {"$sum": {"$toInt": "$raw_data.numcases"}}}},
    {"$group": {"_id": "$_id.gender", 
                "stateCount": {"$push": {"state": "$_id.state", "count": "$count"}}, 
                "total": {"$sum": "$count"}}},
    {"$project": {"_id": 0, "gender": "$_id", 
                  "percentage": {"$map": {"input": "$stateCount", 
                                          "as": "sc", 
                                          "in": {"state": "$$sc.state", 
                                                 "count": {"$multiply": [{"$divide": ["$$sc.count", "$total"]}, 100]}}}}}}
]);
end_time = time.time()
print(end_time-start_time)
# print results
for result in results:
    print(result)
