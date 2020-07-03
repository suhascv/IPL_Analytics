import pymongo


def main():
    myClient = pymongo.MongoClient("mongodb://localhost:27017")
    myDb     = myClient('IPL')
    print('db connected')


    #Creating L1 of item set mining
    




if __name__=='__main__':
    main()