import pymongo

def updateRuns(deliveries,player_score):
    for d,info in deliveries.items():
        if "extras" in info:
            if "noball" in info["extras"] or "wides" in info["extras"]:
                continue
        try:
            player_score[info['batsman']]['runs']+=info['runs']['batsman']
            player_score[info['batsman']]['balls']+=1
        except:
            player_score[info['batsman']]={'runs':info['runs']['batsman'],'balls':1}
    return player_score

def getDocs(player_score,season):
    docs=[]
    for player,info in player_score.items():
        doc={}
        doc['player']=player
        doc['season']=season
        doc['runs']=info['runs']
        doc['strike_rate']=round((info['runs']/info['balls'])*100,2)
        docs.append(doc)
    
    return docs

def main():
    myClient = pymongo.MongoClient("mongodb://localhost:27017")
    myDb     = myClient['IPL']
    Matches  = myDb['matches']
    RunsPerSeason = myDb['runs_per_season']
    print('db connected')

    for season in range(2008,2020):
        data = Matches.aggregate([
            {'$match':{'season':season}},
            {'$project':{'season':1,'1st_innings.deliveries':1,'2nd_innings.deliveries':1}},
            {'$sort':{'season':1}}
        ])
        data = list(data)
        
        player_score={}
        for d in data:
            if '1st_innings' in d:
                player_score=updateRuns(d['1st_innings']['deliveries'],player_score)
            if '2nd_innings' in d:
                player_score=updateRuns(d['2nd_innings']['deliveries'],player_score)
        
        docs=getDocs(player_score,season)
        RunsPerSeason.insert_many(docs)

        
        


if __name__ =='__main__':
    main()