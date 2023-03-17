from pymongo import MongoClient

# connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["covid"]

# execute query and store results
top_states = db.raw_data1.aggregate([
  {"$unwind": "$raw_data"},
  {
    "$group": {
      "_id": "$raw_data.detecteddistrict",
      "totalCases": {
        "$sum": {
          "$toInt": "$raw_data.numcases"
        }
      }
    }
  },
  {
    "$group": {
      "_id": None,
      "nationalAverage": {
        "$avg": "$totalCases"
      },
      "districts": {
        "$push": {
          "district": "$_id",
          "totalCases": "$totalCases"
        }
      }
    }
  },
  {
    "$unwind": "$districts"
  },
  {
    "$match": {
      "gte":["$districts.totalCases","$nationalAverage"]
      }
  },
  {
    "$sort": {
      "districts.totalCases": -1
    }
  },
  {
    "$limit": 10
  },
  {
    "$project": {
      "_id": 0,
      "district": "$districts.district",
      "totalCases": "$districts.totalCases"
    }
  }
])

# print results
for result in top_states:
    print(result)
