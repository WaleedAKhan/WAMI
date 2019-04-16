import twitter
import json
from kafka import KafkaProducer
from kafka.client import SimpleClient
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer

#client = SimpleClient("localhost:9092")
#producer = SimpleProducer(client)

producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))
api = twitter.Api(consumer_key='zfoaXqtplRFtSgjC8RVs1sEbm',
                  consumer_secret='Fz9HzQQToC5b1BjXJ1EwhhwSsib2siM96EVk2oCXkGXcK25eIm',
                  access_token_key='1096068391721287680-gME3FNmXEdp4r7Fb51oDBwAdBVfyob',
                  access_token_secret='AKUN4AQoAOkVGAbzSwqDuMbACtAXR2rELex7PGwDos1Zh',
                  tweet_mode='extended')


s = api.GetStreamFilter(track=['CIHI', 'health canada', 'Canadian Institute for Health Information'],languages=['en'])

for t in s:
    if not t.get('retweeted_status'):
        try:
            
            print(t)
            #print(t.get('id'))
            tweet = api.GetStatus(t.get('id'))
            
            #Serialize to JSON
            data = {}
            data['id'] = tweet.id
            data['text'] = tweet.full_text
            data['hashtags'] = str(tweet.hashtags)
            data['userName'] = tweet.user.name
            data['userScreenName'] = tweet.user.screen_name
            data['createdAt'] = tweet.created_at
            data['userLocation'] = tweet.user.location
            data['userFollowers'] = tweet.user.followers_count
            data['userFollowing'] = tweet.user.friends_count
            if t.get('quoted_status'):
                data['quoted_status'] = tweet.quoted_status.text
                data['quoted_status_full'] = tweet.quoted_status
                print("\n" + data['quoted_status'])
            jsonData = json.dumps(data)
            print(tweet)
            #producer.send_messages('test', t.get('full_text').encode('utf-8'))
            #producer.send_messages('test', tweet.full_text.encode('utf-8'))
            producer.send('test', jsonData)
        
        except Exception as e:
            print("Error occured" + str(e))
        
                  

