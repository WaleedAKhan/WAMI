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
