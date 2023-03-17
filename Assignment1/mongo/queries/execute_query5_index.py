from pymongo import MongoClient
import time

# connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["covid"]
db.state_district_wise.create_index("state")


start_time = time.time()

# execute query and store results
top_states = db.state_district_wise.aggregate([
    { "$project": { "states": { "$objectToArray": "$$ROOT" } } },
    { "$unwind": "$states" },
    { "$replaceRoot": { "newRoot": "$states" } },
    { "$match": { "v.districtData": { "$exists": True } } },
    { "$project": { "state": "$$ROOT.k", "districts": { "$objectToArray": "$$ROOT.v.districtData" } } },
    { "$unwind": "$districts" },
    { "$project": { "state": 1, "district": "$districts.k", "confirmed": "$districts.v.confirmed" } },
    { "$group": { "_id": "$state", "total_confirmed": { "$sum": "$confirmed" }, "cities": { "$push": { "name": "$district", "confirmed": "$confirmed" } } } },
    { "$sort": { "total_confirmed": -1 } },
    { "$limit": 10 },
    {"$unwind": "$cities"},
    {"$sort": {"cities.confirmed": -1}},
    {"$group": {"_id": "$cities.name", "confirmed": {"$sum": "$cities.confirmed"}}},
    {"$sort": {"confirmed": -1}},
    {"$limit": 10}
]
)
end_time = time.time()
print(end_time-start_time)

# print results
for result in top_states:
    print(result)
