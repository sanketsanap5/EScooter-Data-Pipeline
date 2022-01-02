import psycopg2 as db
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import pandas as pd
from pandas import json_normalize
from faker import Faker


#connection
conn_string = "dbname='test' host='localhost' user='postgres' password='postgres'"
conn = db.connect(conn_string)
cur = conn.cursor()
fake = Faker()

#Inserting 1000 users into Posrgres SQL
data=[]
i=2
for r in range(1000):
    data.append((fake.name(),i,fake.street_address(),fake.city(),fake.zipcode()))
    i+=1
fakerData = tuple(data)
query = "insert into users (name,id,street,city,zip) values(%s,%s,%s,%s,%s)"
#print(cur.mogrify(query,fakerData[1]))
#cur.executemany(query,fakerData)
#conn.commit()

#Extracting in DataFrame
query2 = "select * from users"
df= pd.read_sql(query2,conn)
#df.to_json(orient='records')

#Insertng single user into Elastic Search - NoSQL
#es.__version__

es = Elasticsearch()
doc = {
    "name":"sanket",
    "id":1,
    "street":"67 St Germain Street",
    "city":"Boston",
    "zip":"02115"
}

result = es.index(index="users",document=doc)
print(result)

#Inserting 1000 users into Elasticsearch

dataSource=[
    {
        "_index":"users",
        "_source":{
            "name":fake.name(),
            "id":x,
            "street":fake.street_address(),
            "city":fake.city(),
            "zip":fake.zipcode()
        }
    }
    for x in range(1000)
]
es = Elasticsearch()
result = helpers.bulk(es,df.values)
print(result)

#fetching all documents
doc = {
    "query":{
        "match_all":{}
    }
}
res = es.search(index="users",body=doc,size=1)
#print(res['hits']['hits'])
#Loading in DataFrame
df = json_normalize(res['hits']['hits'])
df['_source.id'].value_counts().values==1


#Searching by query
doc = {
    "query":{
        "match":{
            "name":"sanket"
        }
    }
}
res = es.search(index="users",body=doc,size=10)
#print(res['hits']['hits'])
df = json_normalize(res['hits']['hits'])
df

#searching by Lucene sytax
res = es.search(index="users",q="name:sanket",size=10)
df = json_normalize(res['hits']['hits'])
df

