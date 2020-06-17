import pymongo
from datetime import date
"""
creating players collection
"""

def calculateAge(dob):
    """
    calculating age from dob
    """
    dob=dob.split('-')
    born=date(int(dob[0]),int(dob[1]),int(dob[2]))
    today=date.today()
    try:
        #checking if birthyear is a leap year and birthdate is feb 29
        birthday=born.replace(year=today.year)

    except:
        #caught if feb 29 
        birthday = born.replace(month=born.month+1,year=today.year,day=1)
    
    if birthday > today:
        return today.year-born.year-1
    else:
        return today.year-born.year


def main():
    path='../Data/csv-files/'
    data=open(path+'Player.csv')
    batting = open(path+'Batting_Style.csv')
    bowling= open(path+'Bowling_Style.csv')

    batting_skill={}
    bowling_skill={}
    batting.readline()
    bowling.readline()
    for style in batting:
        s=style.strip().split(',')
        batting_skill[s[0]]=s[1]

    for style in bowling:
        s=style.strip().split(',')
        bowling_skill[s[0]]=s[1]


    players=[]
    data.readline()
    for player in data:
        atts=player.strip().split(',')
        
        player_dict={
            '_id':int(atts[0]),
            'name':atts[1],
            'age':calculateAge(atts[2])       
            }
        if atts[3]!='':
            player_dict['batting_hand']=batting_skill[atts[3]]
        if atts[4]!='':    
            player_dict['bowling_skill']=bowling_skill[atts[4]] 
        players.append(player_dict)
    
    myClient=pymongo.MongoClient('mongodb://localhost:27017')
    myDb=myClient['IPL']
    myCol=myDb['players']

    myCol.insert_many(players)

    
if __name__ =='__main__':
    main()