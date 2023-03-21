import paho.mqtt.client as mqtt
import time
import json
import logging
import sys

with open("conf/config.json", "r") as config_file:
    config = json.load(config_file)

broker_address = config['broker_address']
main_topic = config['topics']['main_topic']
begin_client = config['topics']['begin_client']
client_msg_amount = config['msg_details']['amount']
client_amount = config['client_details']['amount']
client_id = config['client_details']['ids']['server']
log = config['client_details']['logs']['server']
msg_amount = client_msg_amount*client_amount

logging.basicConfig(filename = log, filemode = 'a', format = '%(asctime)s %(levelname)s: %(message)s', level = logging.INFO)

logging.info(f"System details: {client_amount} client with {client_msg_amount} messages each, for a total of {msg_amount} messages")

class MQTT_Server:
    def on_connect(self, client, userdata, flags, rc):
        if rc==0:
            logging.info(f"Connected to the broker at {broker_address}")
        else:
            logging.info(f"Error connecting to broker, with code {rc}")

        self.client.subscribe(main_topic, 2)
        logging.info(f"Subscribed to {main_topic} topic with QoS 2")
        self.client.publish(begin_client, None, 0)
        logging.info(f"Sent start order to all the clients")

    def on_message(self, client, userdata, msg):
        self.counter += 1
        self.loss = 100-((self.counter/msg_amount)*100)
        logging.info(f"Received {self.counter} out of {msg_amount} messages, totalling packet loss at {round(self.loss,2)}%")

    def on_disconnect(self, client, userdata, rc):
        logging.info(f"Disconnected from broker at {broker_address}")

    def __init__(self):
        self.counter = 0
        self.loss = 0
        logging.info(f"Creating MQTT Client with ID {client_id}")
        self.client = mqtt.Client(client_id=client_id)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        self.client.connect(broker_address, 1883, 60)
        self.client.loop_forever()

mqtt_server = MQTT_Server()
