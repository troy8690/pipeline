from pymongo import MongoClient
import pandas as pd
import time

def connection():

    CONNECTION_STRING = "mongodb://qatestuser:qatest1234@db.stg.bdb.ai:27017/?authMechanism=DEFAULT"
    client = MongoClient(CONNECTION_STRING)
    result = client['pipeline']['dm'].aggregate([{'$project': {'_id': 0}}])
    result = pd.DataFrame(result)
    result=result.fillna(0)
    print(result)

    result=result.to_dict(orient='records')

    client['pipeline']['mng_tensor_stg'].insert_many(result)
    time.sleep(15)

    client.close()

connection()