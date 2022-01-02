from confluent_kafka import Producer

from faker import Faker
import pandas as pd


p = Producer({
    'bootstrap.servers':'localhost:9092,localhost:9093,localhost:9094',
    })
p.list_topics().topics

def receipt(err,msg):
    if err is not None:
        print('Error: {}'.format(err))
    else:
        print('Message: {}'.format(msg.value()))     

fake = Faker()
name = []
id = []
street = []
city = []
zip = []
for i in range(5):
    name.append(fake.name())
    id.append(i)
    street.append(fake.street_address())
    city.append(fake.city())
    zip.append(fake.zipcode())


df = pd.DataFrame({'name':name,'id':id,'street':street,'city':city,'zip':zip})
p.poll(0)
p.produce('dataEngineering',df.to_json(),callback=receipt)
p.flush()

