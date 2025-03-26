import json
import uuid
import asyncio
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError
from websocket_client import WebSocketClient
from dateutil.parser import *


class PostProducer:

    def __init__(self):
        
        # Kafka Producer configuration
        config = {
            'bootstrap_servers': ['localhost:9092'],
            'key_serializer': lambda key: str(key).encode(),
            'value_serializer': lambda x: json.dumps(x).encode('utf-8')
        }
        
        self.producer = KafkaProducer(**config)

        # Initialize WebSocket client with URI
        self.websocket = WebSocketClient(uri="wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post")

    def publish_posts(self, topic, post):

        # Specify target keys to extract from post
        target_keys = [
            'did', 'time_us', 'kind', 'cid', 'operation', 'createdAt', 'created_at', 'langs', 'text'
        ]

        # Extract specified keys from the post
        extracted_values = self.find_keys(post, target_keys)

        # Check conditions on extracted values
        if extracted_values.get("kind") and extracted_values["kind"][0] == "commit" and \
           extracted_values.get("operation") and extracted_values["operation"][0] == "create":

            # Select correct 'created_at' value
            created_at_val = extracted_values['createdAt'][0] if len(extracted_values['created_at']) == 0 else extracted_values['created_at'][0]

            try:
                # Parse the 'created_at' value to datetime
                dt = parse(created_at_val)
            except ValueError as e:
                print(f"Error parsing date: {e}")
                dt = None

            # Build post dictionary for Kafka
            post_dict = {
                'did': extracted_values["did"][0] if extracted_values.get("did") and extracted_values["did"][0] != "" else None,
                'time_us': extracted_values["time_us"][0] if extracted_values.get("time_us") and extracted_values["time_us"][0] != "" else None,
                'kind': extracted_values["kind"][0] if extracted_values.get("kind") and extracted_values["kind"][0] != "" else None,
                'cid': extracted_values["cid"][0] if extracted_values.get("cid") and extracted_values["cid"][0] != "" else None,
                'operation': extracted_values["operation"][0] if extracted_values.get("operation") and extracted_values["operation"][0] != "" else None,
                'created_at': dt.strftime('%Y-%m-%d %H:%M:%S'),
                'langs': extracted_values["langs"][0] if extracted_values.get("langs") and extracted_values["langs"][0] != "" else None,
            }

            try:
                # Send processed post to Kafka
                message_key = str(uuid.uuid4())
                record = self.producer.send(topic=topic, key=message_key, value=post_dict)
                print(f'Record {message_key} successfully produced at offset {record.get().offset}')
            except KafkaTimeoutError as e:
                print(f"KafkaTimeoutError: Failed to produce record {message_key} - {e}")
            except Exception as e:
                print(f"Error: Failed to produce record {message_key} - {e}")
    
    async def connect_to_server(self):
        # Consume messages from WebSocket
        async for post in self.websocket.messages():
            # Publish each post to Kafka
            await asyncio.to_thread(self.publish_posts, topic='bluesky-raw-posts', post=post)

    def flush_kafka_producer(self):
        # Ensure all messages are sent
        self.producer.flush()

    def close_kafka_producer(self):
        # Close the Kafka producer connection
        self.producer.close()

    def find_keys(self, data, target_keys):
        """
        Recursively search JSON-like structure for target keys.
        Returns a dictionary mapping each target key to a list of found values.
        
        :param data: JSON object as a dict (or list) to search.
        :param target_keys: A list of keys to search for.
        :return: A dict {key: [found_value1, found_value2, ...], ...}
        """
        # Initialize result dictionary for each target key
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
        # Start processing by connecting to server
        asyncio.run(post_producer.connect_to_server())
    except KeyboardInterrupt:
        print("Closing websocket, and kafka producer")
        if 'post_producer' in locals():
            # Ensure Kafka producer is shut down properly
            post_producer.flush_kafka_producer()
            post_producer.close_kafka_producer()
    