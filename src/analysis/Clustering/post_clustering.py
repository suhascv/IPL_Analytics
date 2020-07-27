from pymongo import MongoClient

"""
This is a post clustering step to update player's batting point and batting tier.
once the all player innings are clustered. 
Batting points are calculated from clusters.
Batting points for each innings ranges from 2 to 10 depending on which cluster the innings belong.
The points are updated in players collection.
The players are classified into different tiers based on their batting points.
"""

def main():
    myClient = MongoClient('mongodb://localhost:27017')
    myDb = myClient['IPL']
    myCol = myDb['clustering_data']
    Players =myDb['players']
    bulk=Players.initialize_unordered_bulk_op();


    
    #fetching cluster data
    
    resp=list(myCol.aggregate([
    {
        
        '$group': {
            '_id': '$cluster', 
            'count': {
                '$sum': 1
            }, 
            'avg_fours': {
                '$avg': '$fours'
            }, 
            'avg_sixes': {
                '$avg': '$sixes'
            }, 
            'avg_sr': {
                '$avg': '$strike_rate'
            }, 
            'avg_runs': {
                '$avg': '$runs'
            }
        }
    },
    
     {
        '$sort': {
            'avg_runs': -1, 
            'avg_sr': -1,
            'avg_sixes':-1,
            'avg_fours':-1,
        }
    }
    ]))



    rank={}
    points=[10,8,6,4,2]
    i=0

    #assigning rank to the clusters
    for r in resp:
        rank[r['_id']]=points[i]
        i+=1

    print('points assigned for each cluster')
    print(rank)

    #fetching all players
    all_players=list(Players.aggregate([
    {
        '$group': {
            '_id': None, 
            'players': {
                '$push': '$name'
            }, 
            'count': {
                '$sum': 1
            }
        }
    }
    ]))[0]['players']

    max_point=0
    player_updates=[]

    #calculating batting point of each player
    for player in all_players:
        
        resp=list(myCol.aggregate([
        {
            '$match': {
                'player': player,
            }
        }, {
            '$group': {
                '_id': '$cluster', 
                'count': {
                    '$sum': 1
                }
            }
        }, {
            '$sort': {
                'count': -1
            }
        }
        ]))
    
        
        total_points=0
        for r in resp:
             total_points+=rank[r['_id']]*r['count']

        if total_points>max_point:
            max_point=total_points
        bulk.find({'name':player}).update_one({'$set':{'batting_points':total_points}})
    
    #bulk update players 
    bulk.execute()
    
    print('batting_points updated in players collection')

    tier_range=max_point//5
    upper=max_point
    lower=max_point-tier_range


    #update batting tier based on points obtained
    for i in range(1,6):
        Players.update_many({'batting_points':{'$gt':lower,'$lte':upper}},{'$set':{'batting_tier':i}})
        upper=lower
        lower=lower-tier_range
    
    print('batting_tier updated in players collection')

        
    

    


if __name__ =='__main__':
    main()