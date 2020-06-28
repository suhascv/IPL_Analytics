import pymongo
import pandas as pd
import matplotlib.pyplot as plt

def main():
    myClient = pymongo.MongoClient("mongodb://localhost:27017")
    myDb= myClient['IPL']
    Runs_Conceded = myDb['runs_conceded']


    start=int(input('enter the starting over(min 1,max20) :'))
    stop=int(input('enter the ending over (should be greater than starting over) :'))
    avg_overs={'_id':'$team'}
    overs=[]
    for i in range(start,stop+1):
        avg_overs['over'+str(i)+'_avg']={'$avg':'$over'+str(i)}
        overs.append(i)
    
    query=[{'$match':{'season':{'$in':[2019,2018]}}},
        {'$group':avg_overs}]

    runs_by_team=list(Runs_Conceded.aggregate(query))
    df_dict={'overs':overs}
    teams=[]
    for team in runs_by_team:
        runs_conceded=[]
        for i in range(start,stop+1):
            runs_conceded.append(team['over'+str(i)+'_avg'])
        df_dict[team['_id']]=runs_conceded
        teams.append(team['_id'])
    
    df=pd.DataFrame.from_dict(df_dict)
    
    ax=df.plot.line(x='overs',y=teams,\
        title='Average runs conceded between overs {} and {} by IPL teams'.format(start,stop))
    ax.set_ylabel("avg_runs")
    plt.show()
    
    
    
    

if __name__ =='__main__':
    main()