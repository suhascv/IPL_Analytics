import yaml
import pymongo
import glob
from datetime import date



def getDeliveries(data):
    deliveries={}
    for d in data:
        key = list(d.keys())[0]
        deliveries[str(key).replace('.','_')]=d[key]
    return deliveries



def main():
    myClient = pymongo.MongoClient("mongodb://localhost:27017")
    myDb     = myClient['IPL']
    Matches  = myDb['matches']
    print('db connected')

    all_files=[]
    for file in glob.glob("../Data/ipl/*.yaml"):
        #looping through all files in directory
        file_name=file.strip('../Data/ipl/').strip('.yaml')
        all_files.append(int(file_name))

    #sorting_files
    all_files.sort()

    match_id=1
    matches=[]
    

    for f in all_files:
        file=open("../Data/ipl/"+str(f)+'.yaml')
        data=yaml.load(file, Loader=yaml.FullLoader)
        info= data['info']
        try:
            season =info['dates'][0].year
        except:
            season= int(info['dates'][0].split('-')[0])
        
        match={
                '_id':match_id,
                'season':season,
                'home_team':info['teams'][0],
                'away_team':info['teams'][1],
                'toss_won':info['toss']['winner'],
                'toss_decision':info['toss']['decision'],
            }
            
        if 'winner' in info['outcome']:
            match['winner']=info['outcome']['winner']
            match['won_by']=info['outcome']['by']
            match['player_of_match']=info['player_of_match'][0]
        else:
            match['result']=info['outcome']['result']

            
        if 'innings' in data:
            if len(data['innings'])==1:
                innings1={
                    'team':data['innings'][0]['1st innings']['team'],
                    'deliveries':getDeliveries(data['innings'][0]['1st innings']['deliveries'])
                    }
                match['1st_innings']=innings1

            elif len(data['innings'])==2:
                innings1={
                    'team':data['innings'][0]['1st innings']['team'],
                    'deliveries':getDeliveries(data['innings'][0]['1st innings']['deliveries'])
                    }
                match['1st_innings']=innings1
                innings2={
                    'team':data['innings'][1]['2nd innings']['team'],
                    'deliveries':getDeliveries(data['innings'][1]['2nd innings']['deliveries'])
                    }
                match['2nd_innings']=innings2
            
            elif len(data['innings'])==4:
                #super_overs
                super_ings1 =list(data['innings'][2].keys())[0]
                super_in1={
                            'team':data['innings'][2][super_ings1]['team'],
                            'deliveries':getDeliveries(data['innings'][2][super_ings1]['deliveries'])
                        } 
                super_ings2=list(data['innings'][3].keys())[0]
                super_in2={
                            'team':data['innings'][3][super_ings2]['team'],
                            'deliveries':getDeliveries(data['innings'][3][super_ings2]['deliveries'])
                        }
                match['super_in1']=super_in1
                match['super_in2']=super_in2
        
        matches.append(match)
        match_id+=1
    
    Matches.insert_many(matches)
    print('matches loaded')
    
    
        
        
    

  

        

if __name__ =='__main__':
    main()
    





