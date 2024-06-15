import socket
import time
import json
import random

class Publisher:
    def __init__(self, config):
        self.address = tuple(config['address'])
        self.broker_address = tuple(config['broker_address'])
        self.comments = config['comments']

    def start(self):
        while True:
            comment = random.choice(self.comments)
            self.send_comment(comment)
            time.sleep(random.uniform(1, 2))

    def send_comment(self, comment):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(self.broker_address)
            s.send('publisher'.encode())
            time.sleep(1)
            s.send(json.dumps(comment).encode())
        print(f"Sent comment: {comment['text']} on topic {comment['topic']}")

if __name__ == "__main__":
    with open('publisher_config.json') as f:
        config = json.load(f)
    publisher = Publisher(config)
    publisher.start()
