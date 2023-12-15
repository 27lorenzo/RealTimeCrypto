from kafka import KafkaProducer
from pymongo import MongoClient
import config
from extract_to_mongo import database, collection
import json

client = None
producer = None
kafka_topic = 'coinmarketcap_data'


def create_client(mongo_server):
    global client
    if not client:
        client = MongoClient(host=[mongo_server])


def create_producer():
    global producer
    kafka_bootstrap_servers = 'localhost:9092'
    producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)


def post_to_kafka():
    db = client[database]
    col = db[collection]

    try:
        cursor = col.find({})
        for document in cursor:
            print(f"Sending document to Kafka: {document}")
            document['_id'] = str(document['_id'])
            json_document = json.dumps(document).encode('utf-8')
            producer.send(kafka_topic, value=json_document)
    except Exception as e:
        print(f"Error: {e}")


def main():
    create_client(mongo_server)
    create_producer()
    post_to_kafka()
    client.close()
    producer.close()


if __name__ == '__main__':
    c = config.config()
    mongo_server = c.readh('mongodb', 'server') or 'localhost'

    main()