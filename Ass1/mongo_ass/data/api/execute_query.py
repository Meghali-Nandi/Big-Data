from pymongo import MongoClient

# connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["covid"]

# execute query and store results
results = db.state_test_data.aggregate([
    {
        "$match": {
            "gender": { "$ne": "" },
            "state": { "$ne": "" }
        }
    },
    {
        "$group": {
            "_id": { "state": "$state", "gender": "$gender" },
            "count": { "$sum": 1 }
        }
    }
    # {
    #     "$group": {
    #         "_id": "$_id.state",
    #         "total": { "$sum": "$count" },
    #         "genderCounts": {
    #             "$push": {
    #                 "gender": "$_id.gender",
    #                 "count": "$count"
    #             }
    #         }
    #     }
    # },
    # {
    #     "$project": {
    #         "_id": 0,
    #         "state": "$_id",
    #         "gender": "$genderCounts.gender",
    #         "percent": {
    #             "$map": {
    #                 "input": "$genderCounts",
    #                 "as": "genderCount",
    #                 "in": {
    #                     "$multiply": [
    #                         { "$divide": ["$$genderCount.count", "$total"] },
    #                         100
    #                     ]
    #                 }
    #             }
    #         }
    #     }
    # }
])

# print results
for result in results:
    print(result)
