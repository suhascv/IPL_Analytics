import pymongo
"""
partnership mining using apriori algorithm
"""

def main():
    #database connection
    myClient = pymongo.MongoClient("mongodb://localhost:27017")
    myDb     = myClient['IPL']
    print('db connected')

    min_runs=30
    min_suppport=10
    #Creating L1 of item set mining
    Parnterships = myDb['partnerships']
    partnerships = list(Parnterships.find({'total_runs':{'$gte':min_runs}}))

    
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
    #L1 pruning and load to db
    for item in l1:
        #min support=15
        if l1[item]>min_suppport:
            L1.insert_one({'_id':item,'count':l1[item]})
            lcnt+=1
    
    print('L1 loaded to database, ',lcnt,' items in L1')
    """
    generating l2 candidates from l1 is not required since the pruned version is present in partnerships collection
    further pruning L2 candidates(already in partnerships collection) using L1
    """
    l2={}
    for p in partnerships:
        partners=[]
        partners.append(p['partner1'])
        partners.append(p['partner2'])
        partners.sort()
        
        if partners[0] in l1 and partners[1] in l1:
            pkey='-'.join(partners)
            try:
                l2[pkey]+=1
            except:
                l2[pkey]=1
    

    #L2 connection
    L2 = myDb['L2']
    l2cnt=0
    #L1 pruning and load to db
    for item in l2:
        #min support=15
        if l2[item]>min_suppport:
            L2.insert_one({'_id':item,'count':l2[item]})
            l2cnt+=1

    print('L2 loaded to database, ',l2cnt,' items in L2')

    print('most frequent partnerships with total_runs > ',min_runs)
    most_frequent_partnerships=list(L2.aggregate([{'$sort':{'count':-1}}]))
    
    for fitem in most_frequent_partnerships:
        print(fitem['_id'],' : ',fitem['count'])
        


if __name__=='__main__':
    main()