import json
import uuid
import asyncio
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError
from websocket_client import WebSocketClient
from dateutil.parser import *


class PostProducer:

    def __init__(self):
       
        config = {
            'bootstrap_servers': ['localhost:9092'],
            'key_serializer':   lambda key: str(key).encode(),
            'value_serializer': lambda x: json.dumps(x).encode('utf-8')
        }
        
        self.producer = KafkaProducer(**config)
        self.websocket = WebSocketClient(uri="wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post")

    def publish_posts(self, topic, post):
        try:
            target_keys = [
                'did',
                'time_us',
                'kind',
                'cid',
                'operation',
                'createdAt',
                'created_at',
                'langs',
                'text'
            ]

            extracted_values = self.find_keys(post, target_keys)

            if extracted_values["kind"][0] == "commit" and extracted_values["operation"] is not None and extracted_values["operation"][0] == "create":

                if len(extracted_values['created_at']) == 0:
                    created_at_val = extracted_values['createdAt'][0]
                else:
                    created_at_val = extracted_values['created_at'][0]

                dt = parse(created_at_val)

                post_dict = {
                    'did':        extracted_values["did"][0] if extracted_values.get("did") and len(extracted_values["did"]) > 0 and extracted_values["did"][0] != "" else None,
                    'time_us':    extracted_values["time_us"][0] if extracted_values.get("time_us") and len(extracted_values["time_us"]) > 0 and extracted_values["time_us"][0] != "" else None,
                    'kind':       extracted_values["kind"][0] if extracted_values.get("kind") and len(extracted_values["kind"]) > 0 and extracted_values["kind"][0] != "" else None,
                    'cid':        extracted_values["cid"][0] if extracted_values.get("cid") and len(extracted_values["cid"]) > 0 and extracted_values["cid"][0] != "" else None,
                    'operation':  extracted_values["operation"][0] if extracted_values.get("operation") and len(extracted_values["operation"]) > 0 and extracted_values["operation"][0] != "" else None,
                    'created_at': dt.strftime('%Y-%m-%d %H:%M:%S'),
                    'langs':      extracted_values["langs"][0] if extracted_values.get("langs") and len(extracted_values["langs"]) > 0 and extracted_values["langs"][0] != "" else None,
                    'text':       extracted_values["text"][0] if extracted_values.get("text") and len(extracted_values["text"]) > 0 and extracted_values["text"][0] != "" else None,
                }


                message_key = str(uuid.uuid4())
                record = self.producer.send(topic=topic, key=message_key, value=post_dict)
                
                print(f'Record {message_key} successfully produced at offset {record.get().offset}')

        except KafkaTimeoutError as e:
            print(e)
        

    async def connect_to_server(self):
        async for post in self.websocket.messages():
            self.publish_posts(topic='bluesky-raw-posts', post=post)

    def flush_kafka_producer(self):
        self.producer.flush

    def close_fafka_producer(self):
        self.producer.close()

    def find_keys(self, data, target_keys):
            """
            Recursively search the JSON-like dict (or list) for keys in target_keys.
            Returns a dictionary mapping each target key to a list of all found values.
            
            :param data: JSON object as a dict (or list) to search.
            :param target_keys: A list of keys to search for.
            :return: A dict {key: [found_value1, found_value2, ...], ...}
            """
            # Initialize the result dictionary, one empty list for each target key.
            found = {key: [] for key in target_keys}
            
            def recursive_search(current):
                if isinstance(current, dict):
                    for k, v in current.items():
                        if k in target_keys:
                            found[k].append(v)
                        # Recurse into the value
                        recursive_search(v)
                elif isinstance(current, list):
                    for item in current:
                        recursive_search(item)
            
            recursive_search(data)
            return found

if __name__ == '__main__':

    try:
        post_producer = PostProducer()
        asyncio.run(post_producer.connect_to_server())

    except KeyboardInterrupt:
        print("Closing websocket, and kafka producer")
        post_producer.flush_kafka_producer()
        post_producer.close_fafka_producer()
    