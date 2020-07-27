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

    #fetching attributes in the form of array from the collection
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
            'max_runs':{
                '$max':'$runs'
            },
            'all_strike_rates': {
                '$push': '$strike_rate'
            },
            'max_strike_rate':{
                '$max':'$strike_rate'
            },
            
            'all_sixes':{
                '$push':'$sixes'
            }
            ,
             'all_fours':{
                '$push':'$fours'
            },
            'all_clusters':{
                '$push':'$cluster'
            }
        }
    } 
    ]))[0]

    
    
    df_dict={'runs':resp['all_runs'],
            'strike_rate':resp['all_strike_rates'],
            'sixes':resp['all_sixes'],
            'fours':resp['all_fours'],
            'cluster':resp['all_clusters'],
        }
    
    #creating data frame 
    df=pd.DataFrame(df_dict)

    #runs vs strike_rate
    #normalizing data
    df['normalized_runs']=[x/resp['max_runs'] for x in df['runs']]
    df['normalized_strikerate']=[x/resp['max_strike_rate'] for x in df['strike_rate']]

    #normalized_box_plot
    df.plot(kind='box',y=['normalized_runs','normalized_strikerate'],title="paired wise comparision of normalized runs vs srtike_rate")
    plt.show()

    #scatter plot without normalization
    #scatter plot for comparision of numbers of runs vs strike_rate in an innings.
    df.plot(kind='scatter',x='runs',y='strike_rate',title="paired wise comparision of runs vs strikerate using scatter plot")
    plt.show()


    #scatter plot for comparision of numbers of fours vs sixes in an innings.
    df.plot(kind='box',y=['fours','sixes'],title="paired wise comparision of fours vs sixes")
    plt.show()
    
    
    

    

    

if __name__=='__main__':
    main()

    





