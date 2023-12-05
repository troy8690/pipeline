from pymongo import MongoClient
import pandas as pd

def connection():

    CONNECTION_STRING = "mongodb+srv://root:root@cluster0.acekv.mongodb.net/test?authSource=admin&replicaSet=atlas-svib4e-shard-0&readPreference=primary&ssl=true"
    client = MongoClient(CONNECTION_STRING)
    result = client['bi_testing']['census_data'].aggregate([{'$project': {'_id': 0}}])
    result = pd.DataFrame(result)

    #result = result.fillna(0)
    # print(result)

    #result=result.to_dict(orient='records')
    #print(result)

    pyspark_write(df=result, table="pyspark_job", user="root", password="root", host="cluster0.acekv.mongodb.net",database="bi_testing")

    #client['pipeline']['mng_tensor_stg'].insert_many(result)
    # time.sleep(15)

    # client.close()

# def mongo_writer():
#
#     pyspark_write(df="spark dataframe", table="table name", user="user", password="password", host="host", port="port",
#                   database="database")

def pyspark_write(df, table, user, password, host, database):

    try:
        mongo_uri = "mongodb://"+user+":"+password+"@"+host+":"+"/"+database+"."+table+"?authSource="+database
        df.write.format("com.mongodb.spark.sql.DefaultSource") \
            .mode("append") \
            .option("spark.mongodb.output.uri", mongo_uri).save()
        print("Saved Successfully")
    except Exception as e:
        print(e)

connection()