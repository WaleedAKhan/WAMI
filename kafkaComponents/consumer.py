from kafka import KafkaConsumer
import json

#consumer = KafkaConsumer('test')
consumer = KafkaConsumer('test', auto_offset_reset='earliest', enable_auto_commit=False)
for msg in consumer:
    print(msg)
    print("New Tweet:\n" + msg.value.decode('utf-8') + "\n" )
    #print(type(msg.value.decode('utf-8')))
    try:
        jsonData = json.loads(msg.value.decode('utf-8'))
        print(jsonData)
    except:
        print("Couldnt convert")



                       
