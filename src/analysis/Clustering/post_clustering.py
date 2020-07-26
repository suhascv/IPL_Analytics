from pymongo import MongoClient

def main():
    myClient = MongoClient('mongodb://localhost:27017')
    myDb = myClient['IPL']
    myCol = myDb['clustering_data']
    Players =myDb['players']
    bulk=Players.initialize_unordered_bulk_op();

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
    }, {
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
    for r in resp:
        rank[r['_id']]=points[i]
        i+=1

    print(rank)

    
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

    player_rank={}
    player_updates=[]
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

        player_updates.append({'updateOne':{'pl'}})
        player_rank[player]=total_points
        bulk.find({'name':player}).update_one({'$set':{'batting_points':total_points}})
    
    bulk.execute()

    

    




if __name__ =='__main__':
    main()