import yaml
import pymongo
import glob
from datetime import date


def main():
    myClient = pymongo.MongoClient("mongodb://localhost:27107")
    myDb     = myClient['IPL']
    matches  = myDb['mathces']
    players  = myDb['players']

    all_files=[]
    for file in glob.glob("../Data/ipl/*.yaml"):
        #looping through all files in directory
        file_name=file.strip('../Data/ipl/').strip('.yaml')
        all_files.append(int(file_name))

    all_files.sort()

    match_id=1
    matches=[]
    error_ids=[]
    for f in all_files:
        file=open("../Data/ipl/"+str(f)+'.yaml')
        data=yaml.load(file, Loader=yaml.FullLoader)
        info= data['info']

        try:
            match={
                '_id':match_id,
                'season':info['dates'][0].year,
                'home_team':info['teams'][0],
                'away_team':info['teams'][1],
                'toss_won':info['toss']['winner'],
                'toss_decision':info['toss']['decision'],
                'winner':info['outcome']['winner'],
                'won_by':info['outcome']['by'],
                'player_of_match':info['player_of_match'][0]
                }
        except:
            error_ids.append(f)

        matches.append(match)
        match_id+=1
        
    
    print(error_ids)
        

if __name__ =='__main__':
    main()
    





