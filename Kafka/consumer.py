from confluent_kafka import Consumer

c = Consumer({
    'bootstrap.servers':'localhost:9092,localhost:9093,localhost:9094',
    'group.id':'py-consumer', 'auto.offset.reset':'earliest'
})
print(c.list_topics().topics)
c.subscribe(['dataEngineering'])

while True:
    msg = c.poll(1.0) #Timeout

    if msg is None:
        continue
    if msg.error():
        print('Error: {}'.format(msg.error()))
        continue

    data = msg.value()
    print(data)
c.close()

