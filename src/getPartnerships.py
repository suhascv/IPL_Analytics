import pymongo

"""
1. extracting partnerships from matches
2. update runs scored by each player in players collection
3. load each partnership to partnerships collections 
"""


def getInfo(partner1,partner2):
    """
    returns partnership document
    """
    return {
        partner1:{'runs':0,
                 'balls':0
                 },
        partner2:{'runs':0,
                 'balls':0
                 },
        'total':0
     }


def loadPartnership(partnership,partnership_info,match_id,pid,finishing):
    """
    loads to partnership document to partnerships collection
    """
    partner1=partnership[0]
    partner2=partnership[1]
    total_balls=partnership_info[partner1]["balls"]+partnership_info[partner2]["balls"]
    if partnership_info[partner1]["runs"] == 0:
        partner1_strikerate=0
    elif partnership_info[partner1]["runs"] > 0:
        partner1_strikerate=round((partnership_info[partner1]["runs"]/partnership_info[partner1]["balls"])*100,2)
        
    if partnership_info[partner2]["runs"] == 0:
        partner2_strikerate=0
    elif partnership_info[partner2]["runs"] > 0:
        partner2_strikerate=round((partnership_info[partner2]["runs"]/partnership_info[partner2]["balls"])*100,2)

    if partnership_info['total']==0:
        strikerate=0
    elif partnership_info['total']>0:
        try:
            strikerate=round((partnership_info['total']/total_balls)*100,2)
        except:
            strikerate=0
            pass


    #parnership_schema

    partnership_doc={
        '_id':pid,
        'match_id':match_id,
        'partner1':partner1,
        'partner2':partner2,
        'partner1_runs':partnership_info[partner1]["runs"],
        'partner2_runs':partnership_info[partner2]["runs"],
        'partner1_strikerate':partner1_strikerate,
        'partner2_strikerate':partner2_strikerate,
        'total_runs':partnership_info['total'],
        'balls_faced':total_balls,
        'strikerate':strikerate,
        'finishing':finishing
    }

    return partnership_doc

def updatePlayer(Players,partnership_info,partnership):
    """
    updates players statistics
    """
    partner1=partnership[0]
    partner2=partnership[1]
    Players.update_one(
            {"name":partner1},
            {'$inc':{'runs_scored':partnership_info[partner1]["runs"],
                    'balls_faced':partnership_info[partner1]["balls"]}},
            )

    Players.update_one(
            {"name":partner2},
            {'$inc':{'runs_scored':partnership_info[partner2]["runs"],
                    'balls_faced':partnership_info[partner2]["balls"]}},
            )
    




def inningsParnerships(innings1,chased,match_id,pid,Players):
    #initializing partnership
    partner1= innings1['0_1']["batsman"]
    partner2=innings1['0_1']["non_striker"]
    all_partnerships=[]
    partnership=set([partner1,partner2])
    partnership_info = getInfo(partner1,partner2)
    
    #looping through each ball of the innings and updating partnership_info
    for ball in innings1:
        new_partner1=innings1[ball]["batsman"]
        new_partner2=innings1[ball]["non_striker"]
        new_partnership=set([new_partner1,new_partner2])
        
        #check if partnership is broken
        if new_partnership!=partnership:
            all_partnerships.append(
                loadPartnership(list(partnership),partnership_info,match_id,pid,False))
            updatePlayer(Players,partnership_info,list(partnership))
            partnership=new_partnership
            partnership_info = getInfo(new_partner1,new_partner2)
            pid+=1
        
        partnership_info[new_partner1]["runs"]+=innings1[ball]["runs"]["batsman"]
        partnership_info['total']+=innings1[ball]["runs"]["total"]

        if "extras" in innings1[ball]:
            if 'wides' not in innings1[ball]["extras"]  and 'noball' not in innings1[ball]["extras"]:
                partnership_info[new_partner1]["balls"]+=1
        else:
            partnership_info[new_partner1]["balls"]+=1
        
    all_partnerships.append(loadPartnership(list(partnership),partnership_info,match_id,pid,chased))
    updatePlayer(Players,partnership_info,list(partnership))
    pid+=1

    return pid,all_partnerships

def main():
    myClient = pymongo.MongoClient("mongodb://localhost:27017")
    myDb    = myClient['IPL']
    Matches = myDb['matches']
    Players = myDb['players']
    Partnerships = myDb['partnerships'] 

    print('db connected')

    
    query=[
        {'$project':{'_id':1,
                    "1st_innings.deliveries":1,
                    "2nd_innings.deliveries":1}}
    ]


    deliveries = list(Matches.aggregate(query))
    all_partnerships=[]

    pid=1
    i=0
    for d in deliveries:
        match_id = d["_id"]
        chased=False
        #checking if chasing team has won the match -- to extract finishing partnership
        res=list(Matches.find({'_id':match_id},{'_id':1,'won_by':1}))[0]
        if "won_by" in res:
            if "wickets" in res['won_by']:
                chased=True
        if "1st_innings" in d:
            pid,partnerships=inningsParnerships(d["1st_innings"]["deliveries"],False,match_id,pid,Players)
            all_partnerships+=partnerships
        if "2nd_innings" in d:
            pid,partnerships=inningsParnerships(d["2nd_innings"]["deliveries"],chased,match_id,pid,Players)
            all_partnerships+=partnerships
        i+=1
    
    Partnerships.insert_many(all_partnerships)
    
    print('partnerships collection loaded and players stats updated',i)
        
if __name__=='__main__':
    main()