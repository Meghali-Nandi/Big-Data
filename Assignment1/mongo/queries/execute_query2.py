from pymongo import MongoClient
import time
# connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["covid"]
db.raw_data1.create_index("raw_data.agebracket")
start_time = time.time()
# execute query and store results
results = db["raw_data1"].aggregate([
    {"$unwind": "$raw_data"},
    {
        "$project": {
            "age_group": {
                "$switch": {
                    "branches": [
                        # {"case": {"raw_data.agebracket":{ "$regex": "/.*-.*/"}}, "then":"Unknown"},
                        {"case": {"$and":[{"$lte": [{"$toDouble":"$raw_data.agebracket"}, 5]},{"$gte": [{"$toDouble":"$raw_data.agebracket"}, 0]}]}, "then": "0-5"},
                        {"case": {"$lte": [{"$toDouble":"$raw_data.agebracket"}, 10]}, "then": "6-10"},
                        {"case": {"$lte": [{"$toDouble":"$raw_data.agebracket"}, 15]}, "then": "11-15"},
                        {"case": {"$lte": [{"$toDouble":"$raw_data.agebracket"}, 20]}, "then": "16-20"},
                        {"case": {"$lte": [{"$toDouble":"$raw_data.agebracket"}, 25]}, "then": "21-25"},
                        {"case": {"$lte": [{"$toDouble":"$raw_data.agebracket"}, 30]}, "then": "26-30"},
                        {"case": {"$lte": [{"$toDouble":"$raw_data.agebracket"}, 35]}, "then": "31-35"},
                        {"case": {"$lte": [{"$toDouble":"$raw_data.agebracket"}, 40]}, "then": "36-40"},
                        {"case": {"$lte": [{"$toDouble":"$raw_data.agebracket"}, 45]}, "then": "41-45"},
                        {"case": {"$lte": [{"$toDouble":"$raw_data.agebracket"}, 50]}, "then": "46-50"},
                        {"case": {"$lte": [{"$toDouble":"$raw_data.agebracket"}, 55]}, "then": "51-55"},
                        {"case": {"$lte": [{"$toDouble":"$raw_data.agebracket"}, 60]}, "then": "56-60"},
                        {"case": {"$lte": [{"$toDouble":"$raw_data.agebracket"}, 65]}, "then": "61-65"},
                        {"case": {"$lte": [{"$toDouble":"$raw_data.agebracket"}, 70]}, "then": "66-70"},
                        {"case": {"$lte": [{"$toDouble":"$raw_data.agebracket"}, 75]}, "then": "71-75"},
                        {"case": {"$lte": [{"$toDouble":"$raw_data.agebracket"}, 80]}, "then": "76-80"},
                        {"case": {"$lte": [{"$toDouble":"$raw_data.agebracket"}, 85]}, "then": "81-85"},
                        {"case": {"$lte": [{"$toDouble":"$raw_data.agebracket"}, 90]}, "then": "86-90"},
                        {"case": {"$lte": [{"$toDouble":"$raw_data.agebracket"}, 95]}, "then": "91-95"},
                        {"case": {"$lte": [{"$toDouble":"$raw_data.agebracket"}, 100]}, "then": "96-100"},
                        {"case": {"$gt": [{"$toDouble":"$raw_data.agebracket"}, 100]}, "then": ">100"}
                    ],
                    "default": "Unknown"
                }
            }
        }
    },
    {
        "$group": {
            "_id": "$age_group",
            "count": {"$sum": 1}
        }
    },
    {
        "$sort": {"count": -1}
    },
    {"$limit":1}
])
end_time = time.time()
print(end_time-start_time)
# print results
for result in results:
    print(result)
