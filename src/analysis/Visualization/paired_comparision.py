from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
"""
paired comparision of runs Scored vs Strike Rate of the player's inning.
"""




def main():
    myClient = MongoClient("mongodb://localhost:27017")
    myDb= myClient['IPL']
    RunsScored = myDb['runs_per_match']

    resp=list(RunsScored.aggregate([
    {
        '$group': {
            '_id': None, 
            'all_runs': {
                '$push': '$runs'
            }, 
            'all_strike_rates': {
                '$push': '$strike_rate'
            }
        }
    } 
    ]))[0]

    
    df_dict={'runs':resp['all_runs'],'strike_rate':resp['all_strike_rates']}
    df=pd.DataFrame.from_dict(df_dict)
    df.plot(kind='scatter',x='runs',y='strike_rate')
    plt.show()


if __name__=='__main__':
    main()

    





