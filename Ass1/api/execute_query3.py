from pymongo import MongoClient

# connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["covid"]

# execute query and store results
results = db["deaths_recoveries2"].aggregate([
    {"$unwind": "$deaths_recoveries"},
    {"$group": {
        "_id": "$deaths_recoveries.state",
        "total_cases": {"$sum": 1},
        "total_deaths": {"$sum": {"$cond": {"if": {"$eq": ["$deaths_recoveries.patientstatus", "Deceased"]}, "then": 1, "else": 0}}},
        "total_recovered": {"$sum": {"$cond": {"if": {"$eq": ["$deaths_recoveries.patientstatus", "Recovered"]}, "then": 1, "else": 0}}}
    }},
    {"$addFields": {"recovery_rate": {"$divide": ["$total_recovered", "$total_cases"]}, "death_rate": {"$divide": ["$total_deaths", "$total_cases"]}}},
    {"$sort": {"recovery_rate": -1, "death_rate": 1}},
    {"$limit": 1}
])

# print results
for result in results:
    print(result)
