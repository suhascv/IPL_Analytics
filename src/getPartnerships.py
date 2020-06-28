import pymongo

"""
1. extracting partnerships from matches
2. update runs scored by each player in players collection
3. load each partnership to partnerships collections 
"""


def getInfo(partner1,partner2):
    return {
        partner1:{'runs':0,
                 'balls':0
                 },
        partner2:{'runs':0,
                 'balls':0
                 },
        'total':0
     }

def inningsParnerships(innings1):
    #initializing partnership
    partner1= innings1['0_1']["batsman"]
    partner2=innings1['0_1']["non_striker"]
    partnership=set([partner1,partner2])
    partnership_info = getInfo(partner1,partner2)
    
    #looping through each ball of the innings and updating partnership_info
    for ball in innings1:
        new_partner1=innings1[ball]["batsman"]
        new_partner2=innings1[ball]["non_striker"]
        new_partnership=set([new_partner1,new_partner2])
        
        #check if partnership is broken
        if new_partnership!=partnership:
            print(partnership_info)
            partnership=new_partnership
            partnership_info = getInfo(new_partner1,new_partner2)
            
        
        partnership_info[new_partner1]["runs"]+=innings1[ball]["runs"]["batsman"]
        partnership_info['total']+=innings1[ball]["runs"]["total"]

        if "extras" in innings1[ball]:
            if 'wides' not in innings1[ball]["extras"]  and 'noball' not in innings1[ball]["extras"]:
                partnership_info[new_partner1]["balls"]+=1
        else:
            partnership_info[new_partner1]["balls"]+=1
    
    print(partnership_info)

def main():
    myClient = pymongo.MongoClient("mongodb://localhost:27017")
    myDb    = myClient['IPL']
    Matches = myDb['matches']
    Players = myDb['players']
    Partnerships = myDb['partnerships'] 

    print('db connected')


    query=[
        {'$match':{'$or':[{"1st_innings":{'$exists':True},"2nd_innings":{'$exists':True}}]}},
        {'$project':{'_id':0,"1st_innings.deliveries":1,"2nd_innings.deliveries":1}}
    ]


    deliveries = list(Matches.aggregate(query))

    for d in deliveries:
        #print(d["1st_innings"]["deliveries"])
        inningsParnerships(d["1st_innings"]["deliveries"])
        inningsParnerships(d["2nd_innings"]["deliveries"])
        break;
if __name__=='__main__':
    main()