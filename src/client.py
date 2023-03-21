import paho.mqtt.client as mqtt
import time
import json
import logging
import sys
import threading

with open("conf/config.json", "r") as config_file:
    config = json.load(config_file)

broker_address = config['broker_address']
main_topic = config['topics']['main_topic']
begin_client = config['topics']['begin_client']
msg_amount = config['msg_details']['amount']
msg_size = config['msg_details']['size']
msg_freq = config['msg_details']['freq']
client_id = config['client_details']['ids']['client-0']
log = config['client_details']['logs']['client']

logging.basicConfig(filename = log, filemode = 'a', format = '%(asctime)s %(levelname)s: %(message)s', level = logging.INFO)

logging.info(f"Message details: {msg_amount} messages of {msg_size} bytes with {msg_freq} Hz frequency")

class MQTT_Client:
    def on_connect(self, client, userdata, flags, rc):
        if rc==0:
            logging.info(f"Connected to the broker at {broker_address}")
        else:
            logging.info(f"Error connecting to broker, with code {rc}")

        self.client.subscribe(begin_client, 0)
        logging.info(f"Subscribed to {begin_client} topic with QoS 0")

    def on_message(self, client, userdata, msg):
        logging.info(f"Start order received from the server using topic {str(msg.topic)}")
        self.handler_thread.start()

    def on_publish(self, client, userdata, mid):
        self.counter += 1
        logging.info(f"Published message #{self.counter} to the {main_topic} topic")
        if self.counter == msg_amount:
            logging.info(f"Publish of all {msg_amount} messages complete")

    def on_disconnect(self, client, userdata, rc):
        logging.info(f"Disconnected from broker at {broker_address}")

    def msg_handler(self):
        time.sleep(2)
        payload = bytearray(msg_size)
        logging.info(f"Starting publish of {msg_amount} messages with QoS 2")
        for msg in range(msg_amount):
            self.client.publish(main_topic, payload, qos=2)
            time.sleep(1/msg_freq)

    def __init__(self):
        self.counter = 0
        self.handler_thread = threading.Thread(target = self.msg_handler, args = ())
        logging.info(f"Creating MQTT Client with ID {client_id}")
        self.client = mqtt.Client(client_id=client_id)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_publish = self.on_publish
        self.client.on_message = self.on_message
        self.client.connect(broker_address, 1883, 60)
        self.client.loop_forever()

mqtt_client = MQTT_Client()
