import pandas as pd
import os
from faker import Faker
import psycopg2 as db

baseDir = '/Users/Sanket/Documents/GitHub/Real-Time-Data-Pipeline'
userDataDir = baseDir + '/output/userData/'
fake = Faker()

if not os.path.exists(userDataDir):
    os.mkdir(userDataDir)
for i in range(1,10001):
    data = {
        'name':fake.name(),
        'id':i,
        'street':fake.street_address(),
        'city':fake.city(),
        'zip':fake.zipcode(),
    }
    df = pd.DataFrame([data])
    df.to_json(userDataDir+str(data['id'])+'.json')

userDF = pd.DataFrame()

fileNames = [x for x in os.listdir(userDataDir) if x.endswith('.json')]
for name in fileNames:
    df = pd.read_json(userDataDir+name)
    userDF = userDF.append(other=df,ignore_index=True)
print('Found {} Records In Data Lake '.format(userDF.id.count()))


conn_string = "dbname='test' host='localhost' user='postgres' password='postgres'"
try:
    conn = db.connect(conn_string)
    cur = conn.cursor()
    query = "insert into users (name,id,street,city,zip) values(%s,%s,%s,%s,%s)"
    #print(cur.mogrify(query,tuple(userDF.values)))
    #cur.executemany(query,userDF.values)
    #print(conn.commit())
except Exception as e:
    print(str(e))

pd.read_sql('select count(*) from users;',conn)

cur.execute('truncate table users;')
conn.commit()


userDF.id.count()
print('Found {} Records In Data Lake '.format(userDF.id.count()))
print('Inserting {} Records In Staging DB'.format(userDF.id.count()))

fileNames = [x for x in os.listdir(userDataDir) if x.endswith('.json')]
for name in fileNames:
    if os.path.exists(userDataDir+name):
        os.remove(userDataDir+name)


scooterDF = pd.read_csv(baseDir+'/dataset/scooter.csv')


conn_string = "dbname='test' host='localhost' user='postgres' password='postgres'"
try:
    conn = db.connect(conn_string)
    cur = conn.cursor()
    query = "insert into escooter (month,trip_id,region_id,vehicle_id,started_at,ended_at,  \
        duration,start_location_name,end_location_name,user_id,trip_ledger_id)  \
        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    #print(cur.mogrify(query,tuple(userDF.values)))
    cur.executemany(query,tuple(scooterDF.values))
    conn.commit()
except Exception as e:
    print(str(e))

pd.read_sql('select count(*) from escooter;',conn)

cur.execute('truncate table escooter')
conn.commit()

conn.rollback()