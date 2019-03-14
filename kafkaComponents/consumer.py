from kafka import KafkaConsumer
import json
import re

#consumer = KafkaConsumer('test')
consumer = KafkaConsumer('test', auto_offset_reset='earliest', enable_auto_commit=False)
jsonFile = open("tweetStreamAscii.json", "w+")

def remove_emoji(string):
    emoji_pattern = re.compile("["
                           u"\U0001F600-\U0001F64F"  # emoticons
                           u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                           u"\U0001F680-\U0001F6FF"  # transport & map symbols
                           u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           u"\U00002702-\U000027B0"
                           u"\U000024C2-\U0001F251"
                           "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', string)

i = 0

for msg in consumer:
    #print(msg)
    #print("New Tweet:\n" + msg.value.decode('utf-8') + "\n" )
    #print(type(msg.value.decode('utf-8')))
    try:
        
        jsonString = msg.value.decode('utf-8')
        jsonString = (jsonString.encode('ascii', errors='ignore')).decode('ascii', errors='ignore')
        
        #Need to load twice to get a jsonObject..
        jsonData = json.loads(json.loads(jsonString))
    
        print(jsonData)
        
        jsonFile.write("{\"index\"" + ":{\"_id\":"+str(jsonData['id'])+"}}"+'\n')
        jsonFile.write(str(jsonData))
        jsonFile.write('\n')
        
        
    except:
        print("Couldnt convert")




                       
