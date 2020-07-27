import pymongo
import pandas as pd
import matplotlib.pyplot as plt


"""
number of sixes over all IPL season
"""

def main():
    myClient = pymongo.MongoClient("mongodb://localhost:27017")
    myDb= myClient['IPL']
    RunsPerMatch = myDb['runs_per_match']

    resp = list(RunsPerMatch.aggregate([
    {
        '$group': {
            '_id': '$season', 
            'sixes': {
                '$sum': '$sixes'
            }, 
        }
    }, {
        '$sort': {
            '_id': 1
        }
    }
    ]))

    df_dict={'season':[],'sixes':[]}

    for r in resp:
        df_dict['season'].append(r['_id'])
        df_dict['sixes'].append(r['sixes'])
    
    df =pd.DataFrame(df_dict)

    df.plot(kind='line',x='season',y=['sixes'],title='number of sixes over the seasons')
    plt.show()


if __name__ =='__main__':
    main()