from pymongo import MongoClient





client = MongoClient('mongodb://localhost:27017/')
with client:
    db = client.SWtest
    docs = db.test1.find()
    for doc in docs:
        print('{} {}'.format(doc['id'],doc['content']))