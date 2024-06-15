import socket
import threading
import time
import json

class Broker:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.topics = {}  # {topic: [(timestamp, comment)]}
        self.lock = threading.Lock()

    def handle_publisher(self, conn):
        data = conn.recv(1024).decode()
        comment = json.loads(data)
        topic = comment['topic']
        with self.lock:
            if topic not in self.topics:
                self.topics[topic] = []
            self.topics[topic].append((time.time(), comment['text']))
        print(f"Received comment on topic '{topic}': {comment['text']}")

    def handle_subscriber(self, conn):
        data = conn.recv(1024).decode()
        subscription = json.loads(data)
        topic = subscription['topic']
        with self.lock:
            comments = self.topics.get(topic, [])
        conn.send(json.dumps(comments).encode())
        print(f"Sent comments to subscriber for topic '{topic}'")

    def start(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)
        print(f"Broker listening on {self.host}:{self.port}")

        while True:
            conn, addr = server_socket.accept()
            client_type = conn.recv(1024).decode()
            if client_type == 'publisher':
                threading.Thread(target=self.handle_publisher, args=(conn,)).start()
            elif client_type == 'subscriber':
                threading.Thread(target=self.handle_subscriber, args=(conn,)).start()

if __name__ == "__main__":
    broker = Broker('localhost', 5000)
    broker.start()
