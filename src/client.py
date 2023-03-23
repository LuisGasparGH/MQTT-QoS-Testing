import paho.mqtt.client as mqtt
import time
import json
import logging
import sys
import threading

with open("conf/config.json", "r") as config_file:
    config = json.load(config_file)

client_id = str(sys.argv[1])
log_folder = config['log_folder']
log_name = log_folder + client_id + ".log"
broker_address = config['broker_address']
main_topic = config['topics']['main_topic']
begin_client = config['topics']['begin_client']
client_done = config['topics']['client_done']

logging.basicConfig(filename = log_name, filemode = 'a', format = '%(asctime)s %(levelname)s: %(message)s', level = logging.INFO)

class MQTT_Client:
    def on_connect(self, client, userdata, flags, rc):
        if rc==0:
            logging.info(f"Connected to the broker at {broker_address}")
        else:
            logging.info(f"Error connecting to broker, with code {rc}")

        self.client.subscribe(begin_client, qos=0)
        logging.info(f"Subscribed to {begin_client} topic with QoS 0")

    def on_message(self, client, userdata, msg):
        logging.warning(f"NEW EXECUTION")
        logging.info(f"Start order received from the server using topic {str(msg.topic)}")
        client_config = json.loads(msg.payload)
        self.msg_qos = client_config['msg_qos']
        self.msg_amount = client_config['msg_amount']
        self.msg_size = client_config['msg_size']
        self.msg_freq = client_config['msg_freq']
        
        logging.info(f"Message details: {self.msg_amount} messages of {self.msg_size} bytes using QoS level {self.msg_qos} with {self.msg_freq} Hz frequency")
        self.handler_thread.start()

    def on_publish(self, client, userdata, mid):
        self.counter += 1
        logging.info(f"Published message #{self.counter} to the {main_topic} topic")
        if self.counter == self.msg_amount:
            logging.info(f"Publish of all {self.msg_amount} messages complete")

    def on_disconnect(self, client, userdata, rc):
        logging.info(f"Disconnected from broker at {broker_address}")

    def msg_handler(self):
        time.sleep(2)
        payload = bytearray(self.msg_size)
        logging.info(f"Starting publish of {self.msg_amount} messages with QoS level {self.msg_qos}")
        for msg in range(self.msg_amount):
            self.client.publish(main_topic, payload, qos=self.msg_qos)
            time.sleep(1/self.msg_freq)
        time.sleep(45)
        self.client.publish(client_done, None, qos=0)
        return

    def __init__(self):
        self.msg_qos = 0
        self.msg_amount = 0
        self.msg_size = 0
        self.msg_freq = 0
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
