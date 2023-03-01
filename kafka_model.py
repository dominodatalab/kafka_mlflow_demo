# Import dependencies
import random
from confluent_kafka import TopicPartition,Producer,Consumer
import uuid
import certifi
import codecs
import os
import json
import mlflow
from sklearn.datasets import load_iris
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
import pandas
import pickle
import time
from pyparsing import *

uuid=uuid.uuid1()



is_canary_instance = os.environ.get('IS_CANARY','False').lower() in ('true', '1', 't')

model_topic_name_prefix=os.environ['MODEL_TOPIC_NAME_PREFIX']
inference_group_id = os.environ['INFERENCE_GROUP_ID']

FEATURES_TOPIC=f'{model_topic_name_prefix}-features'
PREDICTION_TOPIC=f'{model_topic_name_prefix}-predictions'
MODEL_UPDATE_TOPIC=f'{model_topic_name_prefix}-updates'

KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_USER_NAME = os.environ.get('KAFKA_USER_NAME')
KAFKA_PASSWORD = os.environ.get('KAFKA_PASSWORD')
KAFKA_FEATURES_TOPIC_PARTITION_RANGE = os.environ.get('KAFKA_FEATURES_TOPIC_PARTITION_RANGE')

import uuid
import time
import os
from confluent_kafka import TopicPartition,Producer,Consumer
import certifi
import threading

uuid=uuid.uuid1()
group_id = f'grp-{uuid}'

latest_version=-1
models = {}



def return_range(strg, loc, toks):
    if len(toks)==1:
        return int(toks[0])
    else:
        return range(int(toks[0]), int(toks[1])+1)
def get_partition_list(s):
    expr = Forward()
    term = (Word(nums) + Optional(Literal('-').suppress() + Word(nums))).setParseAction(return_range)
    expr << term + Optional(Literal(',').suppress() + expr)
    lst =  expr.parseString(s, parseAll=True)
    new_lst = []
    for i in lst:
        if isinstance(i,range):
            for x in i:
                new_lst.append(x)
        else:
            new_lst.append(i)
    return new_lst

features_topic_partition_list = get_partition_list(KAFKA_FEATURES_TOPIC_PARTITION_RANGE)



def get_latest_model(group_id): 
    global latest_version    
    global models
    attempt=1
    
    model_update_consumer_conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                     'sasl.username': KAFKA_USER_NAME,
                     'sasl.password': KAFKA_PASSWORD,
                     'sasl.mechanism': 'PLAIN',
                     'security.protocol': 'SASL_SSL',
                     'ssl.ca.location': certifi.where(),
                     'group.id': group_id,
                     'enable.auto.commit': False,
                     'auto.offset.reset': 'earliest'}
    model_updates_tls = []
    model_updates_tls.append(TopicPartition(MODEL_UPDATE_TOPIC, 0))

    model_update_consumer = Consumer(model_update_consumer_conf)
    model_update_consumer.assign(model_updates_tls)    
    msg = model_update_consumer.poll(timeout=1.0)
    if not msg:
        msg = model_update_consumer.poll(timeout=1.0)    
    while(True):
        if(msg):
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                    (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    sys.stderr.write(f'Error code{msg.error().code()} \n')
            else:
                
                model_json = json.loads(msg.value().decode("utf-8"))
                picked_str=model_json['model']
                model_instance = pickle.loads(codecs.decode(model_json['model'].encode(), "base64"))
                model_version = int(model_json['version'])
                print(f'Retrived{model_version}')
                models[model_version]=model_instance
                if(model_version>latest_version):
                    latest_version = model_version
                model_update_consumer.commit()
        
            msg = model_update_consumer.poll(timeout=1.0) 
        else:
            print('Waiting')
            msg = model_update_consumer.poll(timeout=10.0) 
    print('Returning')
    


def consume_features(group_id:str):  
    print(models)
    print(latest_version)
    if(latest_version<0):
        print('wait for initialization')
        time.sleep(10)
    print('Initialized')
    #latest_version = max(list(models.keys()))
    features_tls = []
    for p in features_topic_partition_list:
        features_tls.append(TopicPartition(FEATURES_TOPIC, p))

    #Only one model instance recieves the message (Each has the SAME consumer group)
    features_consumer_conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                     'sasl.username': KAFKA_USER_NAME,
                     'sasl.password': KAFKA_PASSWORD,
                     'sasl.mechanism': 'PLAIN',
                     'security.protocol': 'SASL_SSL',
                     'ssl.ca.location': certifi.where(),
                     'group.id': group_id,
                     'enable.auto.commit': True,
                     'auto.offset.reset': 'latest'}
    features_consumer = Consumer(features_consumer_conf)    
    features_consumer.assign(features_tls)    

    client_id='client-1'
    producer_conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                     'sasl.username': KAFKA_USER_NAME,
                     'sasl.password': KAFKA_PASSWORD,
                     'sasl.mechanism': 'PLAIN',
                     'security.protocol': 'SASL_SSL',
                     'ssl.ca.location': certifi.where(),
                     'client.id': client_id}    
    predictions_producer = Producer(producer_conf)
    
    msg = None
    msg = features_consumer.poll(1)    
    while(True):
        if msg:            
            features_json = json.loads(msg.value().decode("utf-8"))        
            features_json = json.loads(msg.value().decode("utf-8"))        
            y_hat = models[latest_version].predict([features_json['X']])[0]
            features_consumer.commit()
            prediction_json['CONSUMER_GROUP']=inference_group_id
            prediction_json['FEATURE_INDEX']=str(features_json['index'])
            prediction_json['MODEL_VERSION']=str(latest_version)
            prediction_json['Y_HAT']=str(y_hat)
            p_record = json.dumps(features_json).encode('utf-8')
            predictions_producer.produce(PREDICTION_TOPIC, value=p_record, key=msg.key())
            predictions_producer.flush()
            msg = features_consumer.poll(1)    
            
        else:
            print('waiting for more features')
            msg = features_consumer.poll(10)    
            



def predict(x,version=-1):
    my_x = [x]
    if(version>0):
        if(version>latest_version):
            version = latest_version
    else:
        version=latest_version
        
    if(version>0):        
        y = models[version].predict(my_x)[0]   
        print(y)
        print(version)
        return dict(y=str(y),model_version=str(version))
    else:
        return dict(y=-1,model_version=latest_version,error='No model initialized')
    

def init():
    group_id = f'grp-{uuid}'
    watch_for_model_versions_thread = threading.Thread(target=get_latest_model, args=(group_id,))
    watch_for_model_versions_thread.start()
    time.sleep(10)
    cf = threading.Thread(target=consume_features, args=(inference_group_id,))
    cf.start()
    print('Started')

init()