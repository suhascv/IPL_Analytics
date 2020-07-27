import pymongo
import sys
import math


"""
evaluating the cluster quality by calculating silhoutte co-effecient
"""


def getDistance(point1,point2):
    distance=0
    for c in range(4):
        distance+=(point1[c]-point2[c])**2
    return math.sqrt(distance)

def main():

    myClient=pymongo.MongoClient("mongodb://localhost:27017")
    myDb=myClient['IPL']
    myCol=myDb['clustering_data']

    print("Mongo DB connection established")
    print('calculating silhoutte co-effecient')

    points=list(myCol.find({}))
    Cluster_points=myCol.aggregate([
        {'$group':{'_id':'$cluster', 
                    }
            }
    ])
    columns=[]

    for column in points[0]:
        if column!='centroid':
            columns.append(column)


    s=[]
    N=len(points)


    for point in points:
        same=[]
        other=[]
        
        for p in points:
            if p['cluster']==point['cluster']:
                same.append(getDistance(point['normalizedData'],p['normalizedData']))
            else:
                other.append(getDistance(point['normalizedData'],p['normalizedData']))

        ai=sum(same)/len(same)
        ci=sum(other)/len(other)
        bi=min(other)
        s.append((ai-bi)/max(ai,bi))

    silouhette_coeffecient=sum(s)/N
    print('silouhette coeffecient : ',silouhette_coeffecient)
    #0.332
      

    


if __name__ =='__main__':
    main()