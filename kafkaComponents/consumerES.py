from kafka import KafkaConsumer
import json
from elasticsearch import Elasticsearch
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


analyser = SentimentIntensityAnalyzer()
es = Elasticsearch()
consumer = KafkaConsumer('test', auto_offset_reset='earliest', enable_auto_commit=False)


_id = 0
for msg in consumer:
    
    try:
        #print("New Tweet:\n" + msg.value.decode('utf-8') + "\n" )
        jsonData = json.loads(msg.value.decode('utf-8'))
        jsonBody = json.loads(jsonData)
        
        sentiment = analyser.polarity_scores(jsonBody['text'])
        jsonBody['vaderSentiment'] = sentiment    
        
        if(jsonBody['createdAt']):
            _id = jsonBody['id']
            print(str(_id))
            jsonBody['createdAt'] = jsonBody['createdAt'][:10]
            res = es.index(index="livetweets", id=_id, doc_type="_doc" , body=jsonBody)
        
        
        
    except:
        print("Couldnt load" +str(_id))



                       
