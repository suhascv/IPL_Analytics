import pymongo

"""
Fetching every batting innings of all players across all 12 seasons
This script prepares data for clustering
The following attributes are calculated for each player-innings:
1.Runs                 (Total number of runs scored)
2.Batting StrikeRate   (Indicates the phase in which the runs were scored)
3.Sixes                (Number of sixes in an innings)
4.Fours                (Number of fours in an innings)
"""

def updateRuns(deliveries):
    """
    takes all the deliveries bowled in an innings and calculates runs scored by each player
    """
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

def getDocs(player_score,venue,opponent,venue_type,season,innings):
    """
    prepares docs required for the collection
    """
    docs=[]
    for player,info in player_score.items():
        doc={}
        doc['player']=player
        doc['opponent']=opponent
        doc['venue_type']=venue_type
        doc['venue']=venue
        doc['season']=season
        doc['innings']=innings
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
            {'$project':{'venue':1,
                        '1st_innings':1,
                        '2nd_innings':1,
                        'home_team':1,
                        'away_team':1,
                        'season':1}},
        ])
    data = list(data)
        
    
    for d in data:
        player_score={}
        home_team=d['home_team']
        away_team=d['away_team']
        venue_type='home'
        venue=d['venue']
        season=d['season']
        if '1st_innings' in d:
            if d['1st_innings']['team']==home_team:
                opponent=away_team
            else:
                opponent=home_team
                venue_type='away'
            player_score=updateRuns(d['1st_innings']['deliveries'])
            docs=getDocs(player_score,venue,opponent,venue_type,season,1)
            RunsPerMatch.insert_many(docs)
        if '2nd_innings' in d:
            if d['2nd_innings']['team']==home_team:
                opponent=away_team
            else:
                opponent=home_team
                venue_type='away'
            player_score=updateRuns(d['2nd_innings']['deliveries'])
            docs=getDocs(player_score,venue,opponent,venue_type,season,2)
            RunsPerMatch.insert_many(docs)
        
        

        
        


if __name__ =='__main__':
    main()