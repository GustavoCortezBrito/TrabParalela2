import socket
import time
import json
import random

class Subscriber:
    def __init__(self, config):
        self.address = tuple(config['address'])
        self.broker_address = tuple(config['broker_address'])
        self.topics = config['topics']

    def start(self):
        while True:
            for topic in self.topics:
                self.request_updates(topic)
                time.sleep(random.uniform(1, 2))

    def request_updates(self, topic):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(self.broker_address)
            s.send('subscriber'.encode())
            time.sleep(1)
            s.send(json.dumps({"topic": topic}).encode())
            data = s.recv(1024).decode()
            comments = json.loads(data)
            print(f"Received updates for topic '{topic}': {comments}")

if __name__ == "__main__":
    with open('subscriber_config.json') as f:
        config = json.load(f)
    subscriber = Subscriber(config)
    subscriber.start()
