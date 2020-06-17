import yaml
import glob
import pymongo


    
def main():
    myClient=pymongo.MongoClient("mongodb://localhost:27017")
    

    all_files=[]
    for file in glob.glob("../Data/ipl/*.yaml"):
        #looping through all files in directory
        file_name=file.strip('../Data/ipl/').strip('.yaml')
        all_files.append(int(file_name))
    
    #sort_files by name
    all_files.sort()
    
    players=set()
    errors=[]
    for f in all_files:
        file=open("../Data/ipl/"+str(f)+'.yaml')
        data=yaml.load(file, Loader=yaml.FullLoader)
        try:
            for i in range(2):
                if i==0:
                    innings=data['innings'][0]['1st innings']
                else:
                    innings=data['innings'][1]['2nd innings']
                deleveries=innings['deliveries']
                for d in deleveries:
                    v=list(d.values())
                    players.add(v[0]['batsman'])
                    players.add(v[0]['bowler'])
        except:
            errors.append(f)
    
    print(players)
    print(len(players))
    
    




if __name__=='__main__':
    main()