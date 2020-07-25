from sys import argv

from pymongo import MongoClient
from scipy.spatial import distance

db_config = {
    "host": "mongodb://localhost",
    "port": 27017,
    "db": "IPL"
}

collections = {
    "centroids": "centroids",
    "data": "clustering_data"
}

client = MongoClient(host=db_config['host'], port=db_config['port'])
db = client['IPL']

def get_command_line_params():
    arg_len = len(argv)

    k = 0
    iter = 0

    if arg_len == 3:
        k = argv[1]
        iter = argv[2]
    elif arg_len == 2:
        k = argv[1]
        iter = input('Please enter number of iterations: ')
    else:
        k = input('Please enter number of clusters: ')
        iter = input('Please enter number of iterations: ')

    return (int(k), int(iter))

def get_initial_sample(k: int) -> list:
    sample = db.clustering_data.aggregate([
        {
            '$sample': {
                'size': k
            }
        }
    ])

    return list(sample)

def insert_centroids(points: list):
    docs = []
    _id = 1
    for point in points:
        doc = {}
        doc['_id'] = _id
        doc['point'] = point['normalizedData']
        docs.append(doc)
        _id += 1
    db.drop_collection(collections['centroids'])
    x = db.get_collection(collections['centroids']).insert_many(docs)
    return x.acknowledged, len(x.inserted_ids)

def get_centroids() -> list:
    return list(db.get_collection(collections['centroids']).find({}))

def assign_cluster_centers(docs: list) -> tuple:
    centroids = get_centroids()
    bulk = db.get_collection(collections['data']).initialize_unordered_bulk_op(True)
    for doc in docs:
        doc_point = doc["normalizedData"]
        closest_centroid = None
        closest_distance = float('inf')
        for centroid in centroids:
            dist = distance.euclidean(centroid['point'], doc_point)
            if dist < closest_distance:
                closest_distance = dist
                closest_centroid = centroid
        bulk.find({
            '_id': doc['_id']
        }).update({
            '$set': {
                'cluster': closest_centroid['_id']
            }
        })
    
    x = bulk.execute()
    return x['nMatched'], x['nModified']

def get_clustering_data() -> list:
    return list(db.get_collection(collections['data']).find())

def get_groups_points() -> list:
    return list(db.get_collection('centroids').aggregate([
        {
            '$lookup': {
                'from': 'clustering_data', 
                'localField': '_id', 
                'foreignField': 'cluster', 
                'as': 'data_objects'
            }
        }
    ]))


def update_centroids() -> tuple:
    centroid_groups = get_groups_points()

    bulk = db.get_collection(collections['centroids']).initialize_unordered_bulk_op(True)
    for group in centroid_groups:
        size = len(group['data_objects'])
        new_centroid = [0, 0, 0, 0]
        if (size > 0):
            run_sum = 0
            strike_rate_sum = 0
            six_sum = 0
            four_sum = 0
            for obj in group['data_objects']:
                run_sum += obj['normalizedData'][0]
                strike_rate_sum += obj['normalizedData'][1]
                six_sum += obj['normalizedData'][2]
                four_sum += obj['normalizedData'][3]

            new_centroid = [run_sum / len(group['data_objects']), strike_rate_sum / len(group['data_objects']), six_sum / len(group['data_objects']), four_sum / len(group['data_objects'])]
        bulk.find({
            '_id': group['_id']
        }).update({
            '$set': {
                'point': new_centroid
            }
        })
    
    x = bulk.execute()
    return x['nMatched'], x['nModified']


def do(k: int, iter: int):
    initial_centroids = get_initial_sample(k)
    insert_centroids(initial_centroids)
    docs = get_clustering_data()
    for i in range(iter):
        print(f"***** k = {k} ; iteration = {i + 1} *****")
        match_count, update_count = assign_cluster_centers(docs)
        print(f'Updated {update_count} / {match_count} DOCS with new cluster.')
        match_count, update_count = update_centroids()
        print(f'Updated {update_count} / {match_count} CENTROIDS with new centroid.\n\n')
        if update_count == 0:
            break

def main():
<<<<<<< HEAD
     do(10, 10)

if __name__ == "__main__":
     main()
=======
    k, iterations = get_command_line_params()
    do(k, iterations)

if __name__ == "__main__":
    main()
>>>>>>> bd0863dda3751415472c9a93a6bd944f7a9aa703
