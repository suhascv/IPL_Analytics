import pymongo

"""
runs conceded by each over
"""

def getDocument(runs,oid,match_id,team):
    i=1
    runs_conceded={
        '_id':oid,
        'match_id':match_id,
        'team':team,
    }
    for r in runs:
        runs_conceded['over'+str(i)]=r
        i+=1

    return runs_conceded
        




def getRunsByOver(deliveries):
    runs_by_over=[]
    runs=0
    for d in deliveries:
        if d[-1]=='1':
            runs_by_over.append(runs)
            runs=0
        runs+=deliveries[d]["runs"]["total"]
    runs_by_over.append(runs)
    return runs_by_over[1:]
        





def main():
    myClient = pymongo.MongoClient("mongodb://localhost:27017")
    myDb= myClient['IPL']
    Matches= myDb['matches']
    Runs_conceded = myDb['runs_conceded']
    print('db connected')

    matches = list(Matches.find({}))

    oid=1
    all_overs=[]
    for m in matches:
        home_team=m['home_team']
        away_team=m['away_team']
        match_id=m['_id']
        
        if '1st_innings' in m:
            batting_team=m['1st_innings']['team']
            if batting_team==home_team:
                bowling_team = away_team
            else:
                bowling_team = home_team
            
            runs=getRunsByOver(m['1st_innings']['deliveries'])
            all_overs.append(getDocument(runs,oid,match_id,bowling_team))
            oid+=1
        
        if '2nd_innings' in m:
            batting_team=m['2nd_innings']['team']
            if batting_team==home_team:
                bowling_team = away_team
            else:
                bowling_team = home_team
            
            runs=getRunsByOver(m['2nd_innings']['deliveries'])
            all_overs.append(getDocument(runs,oid,match_id,bowling_team))
            oid+=1

    Runs_conceded.insert_many(all_overs)
    
            


if __name__  == '__main__':
    main()