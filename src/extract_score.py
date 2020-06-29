import pymongo

"""
runs scored by each over at that point of the match
"""

def getDocument(runs,oid,match_id,home_team,away_team,season,batting_team,bowling_team):
    i=1
    docs=[]
    for r in runs:
        runs_scored={
        '_id':oid,
        'match_id':match_id,
        'season':season,
        'home_team':home_team,
        'away_team':away_team,
        'batting_team':batting_team,
        'bowling_team':bowling_team,
        'over':i,
        'runs':r
        }
        i+=1
        oid+=1
        docs.append(runs_scored)

    return oid,docs
        




def getRunsByOver(deliveries):
    runs_by_over=[]
    runs=0
    for d in deliveries:
        if d[-1]=='1':
            runs_by_over.append(runs)
        runs+=deliveries[d]["runs"]["total"]
    runs_by_over.append(runs)
    return runs_by_over[1:]
        





def main():
    myClient = pymongo.MongoClient("mongodb://localhost:27017")
    myDb= myClient['IPL']
    Matches= myDb['matches']
    Runs_Scored = myDb['runs_scored']
    print('db connected')

    matches = list(Matches.find({}))

    oid=1
    all_overs=[]
    for m in matches:
        home_team=m['home_team']
        away_team=m['away_team']
        match_id=m['_id']
        season=m['season']
        if '1st_innings' in m:
            batting_team=m['1st_innings']['team']
            if batting_team==home_team:
                bowling_team = away_team
            else:
                bowling_team = home_team
            
            runs=getRunsByOver(m['1st_innings']['deliveries'])
            oid,docs=getDocument(runs,oid,match_id,home_team,away_team,season,batting_team,bowling_team)
            all_overs+=docs
        
        if '2nd_innings' in m:
            batting_team=m['2nd_innings']['team']
            if batting_team==home_team:
                bowling_team = away_team
            else:
                bowling_team = home_team
            
            runs=getRunsByOver(m['2nd_innings']['deliveries'])
            oid,docs=getDocument(runs,oid,match_id,home_team,away_team,season,batting_team,bowling_team)
            all_overs+=docs
        
    Runs_Scored.insert_many(all_overs)
    
            


if __name__  == '__main__':
    main()