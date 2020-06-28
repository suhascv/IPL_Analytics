import pymongo


"""
extracting partnerships from matches
"""

myClient = pymongo.MongoClient("mongodb://localhost:27017")
myDb    = myClient['IPL']
Matches = myDb['matches']
Players = myDb['players']
Partnerships = myDb['partnerships'] 

print('db connected')


query={
      
}




