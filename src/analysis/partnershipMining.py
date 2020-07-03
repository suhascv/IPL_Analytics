import pymongo
"""
partnership mining using apriori algorithm
"""

def main():
    #database connection
    myClient = pymongo.MongoClient("mongodb://localhost:27017")
    myDb     = myClient['IPL']
    print('db connected')


    #Creating L1 of item set mining
    Parnterships = myDb['partnerships']
    partnerships = list(Parnterships.find({'total_runs':{'$gte':20}}))

    
    l1={}
    for p in partnerships:
        try:
            l1[p['partner1']]+=1
        except:
            l1[p['partner1']]=1
        try:
            l1[p['partner2']]+=1
        except:
            l1[p['partner2']]=1

    #L1 connection
    L1 = myDb['L1']  
    lcnt=0
    for item in l1:
        #min support=15
        if l1[item]>15:
            L1.insert_one({'player':item,'count':l1[item]})
            lcnt+=1
    
    print('L1 loaded to database, ',lcnt,' items in L1')

    #pruning L2 candidates(already in partnerships collection) using L1
    for p in partnerships:
        pass






if __name__=='__main__':
    main()