import pymongo
from itertools import combinations

"""
partnership mining using apriori algorithm
Initially, we will filter the partnerships with total runs > 30. 
The algorithm works sligthly different from usual apriori, 
here we have fixed number of items in a partnership(transaction),
thus the algorithm stops at level 3. The minimum support is set to 15.

At Level 1 we will get the the individual players/venues involved in atleast 15 partnerships(30+ run).
At Level 2 we will have the player-venue/player1-player2 involved in atleast 15 partnerships(30+ run).
At Level 3 we will have the most frequent partnerships(partner1,partner2,venue)
(note: at last level we change min support to 5 as the number of matches played by player pair at particular venue decreases).
"""


def removeVenue(itemset,venues):
    for i in itemset:
        if i in venues:
            itemset.remove(i)
            return itemset,i
    return itemset,None



def queryGenerator(itemset,venue,min_runs):
    """
    returns generalized query to do the following
        1)to prune invalid candidates.
        2)to prune candidates(partnership) with runs < min runs.
    """
    level=len(itemset)
    partners=[[] for k in range(level)]
    for j in range(level):
        for i in range(level):
            partners[i].append({'partner'+str(j+1):itemset[i]})
        k=itemset.pop(0)
        itemset.append(k)
    ors=[]
    for cond in partners:
        ors.append({'$and':cond})
    
    match={}
    match['$or']=ors
    match['total_runs']={'$gte':30}
    if venue:
        match['venue']=venue

    return match


def getDoucuments(data,level,venues,myCol,min_runs):
    #generates candidates for level_i
    myDocs = {}
    if level ==1:
        for p in data:
            try:
                myDocs[p['partner1']]+=1
            except:
                myDocs[p['partner1']]=1
            try:
                myDocs[p['partner2']]+=1
            except:
                myDocs[p['partner2']]=1
            try:
                myDocs[p['venue']]+=1
            except:
                myDocs[p['venue']]=1
    
    else:
        items=list(combinations(data,level))
        for itemset in items:
            itemset=list(itemset)
            itemset.sort()
            partners='-'.join(itemset)
            count=0
            itemset,is_venue=removeVenue(itemset,venues)
            query=[
                    {'$match':queryGenerator(itemset,is_venue,min_runs),
                    }
            ]
            resp=list(myCol.aggregate(query))
            count+=len(resp)
            myDocs[partners]=count
              
    return myDocs


def pruneCandidates(candidates,min_support):
    #prunes invalid candidates and candidates with support < min support
    prunedDocs=[]
    for c in candidates:
        if candidates[c]>=min_support:
            doc={'count':candidates[c]}
            i=1
            for item in c.split('-'):
                doc['item'+str(i)]=item
                i+=1
            prunedDocs.append(doc)
    return prunedDocs

def getData(level,resp):
    data=set()
    for r in resp:
        for i in range(level):
            data.add(r['item'+str(i+1)])
    return list(data)

def main():
    #database connection
    myClient = pymongo.MongoClient("mongodb://localhost:27017")
    myDb     = myClient['IPL']
    print('db connected')

    min_runs = 30
    min_support = 10
    Parnterships = myDb['partnerships']
    partnerships = list(Parnterships.find({'total_runs':{'$gte':min_runs}}))
    venues = Parnterships.distinct("venue")


    levels=[]
    print('number of itemsets before Level1 of apriori :',len(partnerships))
    data=partnerships
    
    
    #from level 1 to 3
    for i in range(1,4):
        if i==1 or i==2:
            support=10
        if i==3:
            support=5
            
        Level = myDb['level'+str(i)]
        #generate candidates
        candidates=getDoucuments(data,i,venues,Parnterships,min_runs)
        
        #prune candidates of level i
        pruned=pruneCandidates(candidates,support)
        print('number of itemsets after Level'+str(i),':',len(pruned))
        
        #creating_new_collection for items in each level.
        Level.insert_many(pruned)

        resp=list(Level.find({},{'_id':0,'count':0}))
        data = getData(i,resp)
        

    answer = list(Level.aggregate([{'$project':{'_id':0}},{'$sort':{'count':-1}}]))
    print(answer)
   



if __name__=='__main__':
    main()