import sys, os, inspect
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

server = MongoClient(host=db_config['host'], port=db_config['port'])
db = server['IPL']

def get_initial_sample(k: int) -> list:
    sample = db.clustering_data.aggregate([
        {
            '$sample': {
                'size': 10
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
    centroids_cursor = db.get_collection(collections['centroids']).find({})
    return list(doc['point'] for doc in centroids_cursor)

def assign_cluster_centers(docs: list) -> list:
    centroids = get_centroids()
    bulk = db.get_collection(collections['data']).initialize_unordered_bulk_op(True)
    for doc in docs:
        doc_point = doc["normalizedData"]
        closest_centroid = [float('inf'), float('inf'), float('inf'), float('inf')]
        closest_distance = float('inf')
        for centroid in centroids:
            dist = distance.euclidean(centroid, doc_point)
            if dist < closest_distance:
                closest_distance = dist
                closest_centroid = centroid
        doc['cluster'] = closest_centroid
        bulk.find({
            '_id': doc['_id']
        }).update({
            '$set': {
                'cluster': doc['cluster']
            }
        })
    
    x = bulk.execute()
    return x['nMatched'], x['nModified']

def get_clustering_data():
    return list(db.get_collection(collections['data']).find())

def update_centroids():
    centroid_groups = list(db.get_collection('centroids').aggregate([
        {
            '$lookup': {
                'from': 'clustering_data', 
                'localField': 'point', 
                'foreignField': 'cluster', 
                'as': 'data_objects'
            }
        }
    ]))

    bulk = db.get_collection(collections['centroids']).initialize_unordered_bulk_op(True)
    for group in centroid_groups:
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
        print(f'Updated {update_count} / {match_count} CENTROIDS with new centroid.')

        
def main():
    do(10, 10)

if __name__ == "__main__":
    main()