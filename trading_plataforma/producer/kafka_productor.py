from kafka import KafkaProducer
import json

class EventProducer:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=["localhost:9092"],
                                      value_serializer=lambda event: json.dumps(event).encode("utf-8"))
    def publish(self,event):
        self.producer.send("ticks", event)
        self.producer.flush()
    def close(self):
        self.producer.close()