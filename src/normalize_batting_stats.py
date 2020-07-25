import pymongo

db_config = {
    "host": "mongodb://localhost",
    "port": 27017,
    "db": "IPL"
}

def main():
    myClient = pymongo.MongoClient(host=db_config["host"], port=db_config["port"])
    print(f"Connected to the server {db_config['host']}/{db_config['port']}!")
    
    myDb = myClient[db_config['db']]
    print(f"Connected to the db {db_config['db']}!")

    myDb.runs_per_match.aggregate([
        {
            '$group': {
                '_id': None, 
                'maxRuns': {
                    '$max': '$runs'
                }, 
                'minRuns': {
                    '$min': '$runs'
                }, 
                'maxStrikeRate': {
                    '$max': '$strike_rate'
                }, 
                'minStrikeRate': {
                    '$min': '$strike_rate'
                }, 
                'mostSixes': {
                    '$max': '$sixes'
                }, 
                'leastSixes': {
                    '$min': '$sixes'
                }, 
                'mostFours': {
                    '$max': '$fours'
                }, 
                'leastFours': {
                    '$min': '$fours'
                }, 
                'data': {
                    '$push': '$$ROOT'
                }
            }
        }, {
            '$unwind': {
                'path': '$data'
            }
        }, {
            '$addFields': {
                'data.normalizedData': [{
                        '$divide': [
                            {
                                '$subtract': [
                                    '$data.runs', '$minRuns'
                                ]
                            }, {
                                '$subtract': [
                                    '$maxRuns', '$minRuns'
                                ]
                            }
                        ]
                    },{
                        '$divide': [
                            {
                                '$subtract': [
                                    '$data.strike_rate', '$minStrikeRate'
                                ]
                            }, {
                                '$subtract': [
                                    '$maxStrikeRate', '$minStrikeRate'
                                ]
                            }
                        ]
                    },{
                        '$divide': [
                            {
                                '$subtract': [
                                    '$data.sixes', '$leastSixes'
                                ]
                            }, {
                                '$subtract': [
                                    '$mostSixes', '$leastSixes'
                                ]
                            }
                        ]
                    },{
                        '$divide': [
                            {
                                '$subtract': [
                                    '$data.fours', '$leastFours'
                                ]
                            }, {
                                '$subtract': [
                                    '$mostFours', '$leastFours'
                                ]
                            }
                        ]
        }]
        }}, {
            '$replaceRoot': {
                'newRoot': '$data'
            }
        }, {
            '$out': 'clustering_data'
        }
    ])
    print('Clustering data ready!')

if __name__ == "__main__":
    main()