from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt

"""
3D plot to visualize the created clsuters
"""

def main():
    myClient = MongoClient("mongodb://localhost:27017")
    myDb= myClient['IPL']
    RunsScored = myDb['clustering_data']

    #fetching the data in form of array
    resp=list(RunsScored.aggregate([
    
    {
        '$group': {
            '_id': None, 
            'all_runs': {
                '$push': '$runs'
            }, 
            'all_strike_rates': {
                '$push': '$strike_rate'
            },
            
            'all_sixes':{
                '$push':'$sixes'
            },
            'all_fours':{
                '$push':'$fours'
            }
            ,
            'all_clusters':{
                '$push':'$cluster'
            }
        }
    } 
    ]))[0]

    
    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d')
    

    #preparing data frame
    df_dict={'runs':resp['all_runs'],
            'strike_rate':resp['all_strike_rates'],
            'sixes':resp['all_sixes'],
            'cluster':resp['all_clusters'],
            'fours':resp['all_fours']}
    df=pd.DataFrame(df_dict)
    df['boundaries']=[df['sixes'][i]+df['fours']  for i in range(len(df.sixes))]
    #markers
    df_dict={1:'o',2:'v',3:'s',4:'^',5:'d'}
    for kind in df_dict:
        d = df[df.cluster==kind]
        ax.scatter( d.runs,d.strike_rate,d.sixes,
                marker = df_dict[kind])
    ax.set_xlabel('Runs')
    ax.set_ylabel('Strike_Rate')
    ax.set_zlabel('Boundaries')
    ax.set_title('Clustered Innings')
    plt.show()
    

    

if __name__=='__main__':
    main()