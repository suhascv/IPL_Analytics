import pymongo

def updateRuns(deliveries):
    player_score={}
    for d,info in deliveries.items():
        if "extras" in info:
            if "noball" in info["extras"] or "wides" in info["extras"]:
                continue
        try:
            player_score[info['batsman']]['runs']+=info['runs']['batsman']
            player_score[info['batsman']]['balls']+=1
        except:
            player_score[info['batsman']]={
                                            'runs':info['runs']['batsman'],
                                            'balls':1,
                                            'sixes':0,
                                            'fours':0
                                            }

        if info['runs']['batsman']==4:
            player_score[info['batsman']]['fours']+=1
        if info['runs']['batsman']==6:
            player_score[info['batsman']]['sixes']+=1
        
    return player_score

def getDocs(player_score):
    docs=[]
    for player,info in player_score.items():
        doc={}
        doc['player']=player
        doc['runs']=info['runs']
        doc['strike_rate']=round((info['runs']/info['balls'])*100,2)
        doc['sixes']=info['sixes']
        doc['fours']=info['fours']
        docs.append(doc)
    
    return docs

def main():
    myClient = pymongo.MongoClient("mongodb://localhost:27017")
    myDb     = myClient['IPL']
    Matches  = myDb['matches']
    RunsPerMatch = myDb['runs_per_match']
    print('db connected')

    
    data = Matches.aggregate([
            {'$project':{'1st_innings.deliveries':1,'2nd_innings.deliveries':1}},
        ])
    data = list(data)
        
    
    for d in data:
        player_score={}
        if '1st_innings' in d:
            player_score=updateRuns(d['1st_innings']['deliveries'])
            docs=getDocs(player_score)
            RunsPerMatch.insert_many(docs)
        if '2nd_innings' in d:
            player_score=updateRuns(d['2nd_innings']['deliveries'])
            docs=getDocs(player_score)
            RunsPerMatch.insert_many(docs)
        
        

        
        


if __name__ =='__main__':
    main()