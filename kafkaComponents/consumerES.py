from kafka import KafkaConsumer
import json
from elasticsearch import Elasticsearch
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import boto3
import requests
from ibm_watson import NaturalLanguageUnderstandingV1
from ibm_watson.natural_language_understanding_v1 import Features, EntitiesOptions, KeywordsOptions, SentimentOptions



analyser = SentimentIntensityAnalyzer()
es = Elasticsearch()
consumer = KafkaConsumer('test',auto_offset_reset='latest', enable_auto_commit=True)
awsClient = boto3.client('comprehend')


def getAWSSentiment(text):
    return awsClient.detect_sentiment(Text=text,LanguageCode='en')    

def getAWSEntities(text):
    return awsClient.detect_entities(Text=text,LanguageCode='en')

def getAWSKeyPhrases(text):
    return awsClient.detect_key_phrases(Text=text, LanguageCode='en')


def getCustomNLPScore(text):
    text = text.replace("#", "")
    str = requests.get("http://localhost:9000/"+text.replace('\n', ' ')).text
    print(str)
    #str = urllib.request.urlopen("http://localhost:9000/"+text).read().decode('utf-8')
    return float(str)


watsonService = NaturalLanguageUnderstandingV1(
    version='2018-03-16',
    # url is optional, and defaults to the URL below. Use the correct URL for your region.
    url='https://gateway-wdc.watsonplatform.net/natural-language-understanding/api/v1/analyze?version=2018-11-16',
    iam_apikey='nyPJioayn09Yw1MQssCa-vjUh5NUI313PFo9mbPJOdZU')

def getWatsonNLP(text):
    
    response = watsonService.analyze(
    text=text,
    features=Features(entities=EntitiesOptions(),
                      keywords=KeywordsOptions(),
                      sentiment=SentimentOptions())).get_result()

    return response


_id = 0
for msg in consumer:
    
    try:
        #print("New Tweet:\n" + msg.value.decode('utf-8') + "\n" )
        jsonData = json.loads(msg.value.decode('utf-8'))
        jsonBody = json.loads(jsonData)
        
        sentiment = analyser.polarity_scores(jsonBody['text'])
        jsonBody['vaderSentiment'] = sentiment    
       
        #Add new calculated field Vader sentiment rating
        pos = sentiment.get('pos')
        neg = sentiment.get('neg')
        neu = sentiment.get('neu')

        if (pos > neg and pos > neu):
            rating = "POSITIVE"
        elif (neg > pos and neg > neu):
            rating = "NEGATIVE"
        elif (neu > pos and neu > neg):
            rating = "NEUTRAL"
        else:
            rating = "UNKNOWN"

        jsonBody['vaderSentimentRating'] = rating


 
        if(jsonBody['createdAt']):
            _id = jsonBody['id']
            #print(str(_id))
            #jsonBody['createdAt'] = jsonBody['createdAt'][:10]
            
            #Get AWS Sentiment
            awsSentiment = getAWSSentiment(jsonBody['text'])
            jsonBody['awsSentiment'] = awsSentiment['SentimentScore']
            jsonBody['awsSentimentRating'] = awsSentiment['Sentiment']
            
            #Get AWS Entities
            awsEntities = getAWSEntities(jsonBody['text'])
            jsonBody['awsEntities'] = awsEntities['Entities']

            #Get AWS Key Phrases
            awsKeyPhrases = getAWSKeyPhrases(jsonBody['text'])
            jsonBody['awsKeyPhrases'] = awsKeyPhrases['KeyPhrases']

            #Get CustomNLPScore
            customNLP = getCustomNLPScore(jsonBody['text'])
            jsonBody['customNLP'] = customNLP

            #sent140polarity
            #print(jsonBody['text'])
            sent140ReqData = '{\'data\': [{\'text\': \'' + jsonBody['text'].replace("'","\\'") + '\'}]}'
            sent140ReqData = sent140ReqData.encode('utf-8')
            params = (('appid', 'twitter.dev.cihi@gmail.com'),)
            response = requests.post('http://www.sentiment140.com/api/bulkClassifyJson', params=params, data=sent140ReqData)
            jsonresp = json.loads(response.text)
            polarity = jsonresp['data'][0]['polarity']
            #print(jsonresp)
            jsonBody['sent140Polarity'] = polarity
                        
            #Watson Sentiment
            watsonResponse = getWatsonNLP(jsonBody['text'])        
            jsonBody['watsonSentiment'] = watsonResponse['sentiment']
            jsonBody['watsonKeywords'] = watsonResponse['keywords']
            jsonBody['watsonEntities'] = watsonResponse['entities']      

            res = es.index(index="livetweets", id=_id, doc_type="_doc" , body=jsonBody)
        
        
        
    except Exception as e:
        print("Couldnt load" +str(_id) + "  " + str(e))



                       
