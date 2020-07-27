from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt





def main():
    myClient = MongoClient("mongodb://localhost:27017")
    myDb= myClient['IPL']
    RunsScored = myDb['clustering_data']




    """
    paired comparision of srtike_rate vs runs
    """
    resp=list(RunsScored.aggregate([
    {'$sort':{
        'runs':-1,
        'strike_rate':-1
    }
    },

    {
        '$group': {
            '_id': None, 
            'all_runs': {
                '$push': '$runs'
            }, 
            'all_strike_rates': {
                '$push': '$strike_rate'
            },
            'all_clusters':{
                '$push':'$cluster'
            }
        }
    } 
    ]))[0]

    
    df_dict={'runs':resp['all_runs'],'strike_rate':resp['all_strike_rates'],'cluster':resp['all_clusters']}
    df=pd.DataFrame(df_dict)
    df_dict={1:'o',2:'v',3:'s',4:'^',5:'d'}
    for kind in df_dict:
        d = df[df.cluster==kind]
        plt.scatter( d.strike_rate, d.runs,
                marker = df_dict[kind])
    plt.show()

    """
    paired comparision of sixes vs fours
    """
    resp=list(RunsScored.aggregate([
    {'$sort':{
        'sixes':-1,
        'fours':-1
    }
    },    
    {
        '$group': {
            '_id': None, 
            'sixes': {
                '$push': '$sixes'
            }, 
            'fours': {
                '$push': '$fours'
            },
            'all_clusters':{
                '$push':'$cluster'
            }
        }
    } 
    ]))[0]

    
    df_dict={'sixes':resp['sixes'],'fours':resp['fours'],'cluster':resp['all_clusters']}
    df=pd.DataFrame(df_dict)
    df_dict={1:'o',2:'v',3:'s',4:'^',5:'d'}
    for kind in df_dict:
        d = df[df.cluster==kind]
        plt.scatter(x=d.fours,y= d.sixes, 
                marker = df_dict[kind])
    plt.show()


if __name__=='__main__':
    main()

    





