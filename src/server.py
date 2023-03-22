import paho.mqtt.client as mqtt
import time
import json
import logging
import sys

with open("conf/config.json", "r") as config_file:
    config = json.load(config_file)

client_id = str(sys.argv[1])
log_folder = config['log_folder']
log_name = log_folder + client_id + ".log"
broker_address = config['broker_address']
main_topic = config['topics']['main_topic']
begin_client = config['topics']['begin_client']

client_amount = config['system_details']['client_amount']
msg_qos = config['system_details']['msg_qos']
msg_amount = config['system_details']['msg_amount']
msg_size = config['system_details']['msg_size']
msg_freq = config['system_details']['msg_freq']
total_msg_amount = msg_amount * client_amount

logging.basicConfig(filename = log_name, filemode = 'a', format = '%(asctime)s %(levelname)s: %(message)s', level = logging.INFO)
logging.warning(f"NEW EXECUTION")
logging.info(f"System details: {client_amount} clients with {msg_amount} messages each using QoS level {msg_qos}, for a total of {total_msg_amount} messages")

class MQTT_Server:
    def on_connect(self, client, userdata, flags, rc):
        if rc==0:
            logging.info(f"Connected to the broker at {broker_address}")
        else:
            logging.info(f"Error connecting to broker, with code {rc}")

        self.client.subscribe(main_topic, qos=msg_qos)
        logging.info(f"Subscribed to {main_topic} topic with QoS level {msg_qos}")
        
        client_config = json.dumps(config['system_details'])
        self.client.publish(begin_client, client_config, qos=0)
        logging.info(f"Sent configuration and start order to all the clients")

    def on_message(self, client, userdata, msg):
        self.counter += 1
        self.loss = 100-((self.counter/msg_amount)*100)
        logging.info(f"Received {self.counter} out of {msg_amount} messages, totalling packet loss at {round(self.loss,3)}%")

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
