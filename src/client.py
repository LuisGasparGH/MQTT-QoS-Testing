# Import of all necessary packages and libraries
import paho.mqtt.client as mqtt
import time
import json
import logging
import sys
import threading

# Read the configuratoin file, which includes used topics
with open("conf/config.json", "r") as config_file:
    config = json.load(config_file)

# Stores all static variables needed
client_id = str(sys.argv[1])
log_folder = config['log_folder']
log_name = log_folder + client_id + ".log"
broker_address = config['broker_address']
main_topic = config['topics']['main_topic']
begin_client = config['topics']['begin_client']
client_done = config['topics']['client_done']

# Configures the logger which will contain all execution details for further analysis
logging.basicConfig(filename = log_name, filemode = 'a', format = '%(asctime)s %(levelname)s: %(message)s', level = logging.INFO)

# Class of the client code
class MQTT_Client:
    # Callback for when the client object successfully connects to the broker
    def on_connect(self, client, userdata, flags, rc):
        if rc==0:
            # Upon successful connection, the client subscribes to the start topic waiting for order
            logging.info(f"Connected to the broker at {broker_address}")
            self.client.subscribe(begin_client, qos=0)
            logging.info(f"Subscribed to {begin_client} topic with QoS 0")
        else:
            # In case of error during connection the log will contain the error code for debugging
            logging.info(f"Error connecting to broker, with code {rc}")

    # Callback for when the client object receives a message, which includes topic and payload
    def on_message(self, client, userdata, msg):
        # In case of a None payload, means the server has no more executions to order and is telling the clients to finish
        if msg.payload == None:
            self.cleanup()
        # If the payload is not empty, it will contain configuration information for the next run, such as message size, amount, publishing frequency and QoS level to be used
        else:
            logging.warning(f"NEW EXECUTION")
            logging.info(f"Start order received from the server using topic {str(msg.topic)}")
            # Declares the thread where the run handler will run. Has to be done everytime a new run is executed
            self.handler_thread = threading.Thread(target = self.run_handler, args = ())
            # Parses the payload to a dictionary, and stores all the needed settings
            client_config = json.loads(msg.payload)
            self.msg_qos = client_config['msg_qos']
            self.msg_amount = client_config['msg_amount']
            self.msg_size = client_config['msg_size']
            self.msg_freq = client_config['msg_freq']
            # Logs the run details and starts the run handler thread
            logging.info(f"Message details: {self.msg_amount} messages of {self.msg_size} bytes using QoS level {self.msg_qos} with {self.msg_freq} Hz frequency")
            self.handler_thread.start()

    # Callback for the when the client object successfully completes the publishing of a message (including necessary handshake for QoS levels 1 and 2)
    def on_publish(self, client, userdata, mid):
        # The client contains a counter of sent messages for logging purposes
        self.sent_counter += 1
        logging.info(f"Published message #{self.counter} to the {main_topic} topic")
        if self.counter == self.msg_amount:
            logging.info(f"Publish of all {self.msg_amount} messages complete")

    # Callback for when the client object successfully disconnects from the broker
    def on_disconnect(self, client, userdata, rc):
        logging.info(f"Disconnected from broker at {broker_address}")

    # Run handler function, used to execute each run with the information received from the server
    def run_handler(self):
        # As per the requirements, the client sleeps for 2 seconds after it receives the order to start, and after that creates the payload with the specific size for the run
        time.sleep(2)
        payload = bytearray(self.msg_size)
        logging.info(f"Starting publish of {self.msg_amount} messages with QoS level {self.msg_qos}")
        # This cycle is iterated as many times as messages that need to be published in this run
        for msg in range(self.msg_amount):
            # Messages are published with the correct QoS, and the thread sleeps for the necessary time to meet the frequency
            self.client.publish(main_topic, payload, qos=self.msg_qos)
            time.sleep(1/self.msg_freq)
        # After all messages are sent, the client waits for a period of 45 seconds to make sure the server is finished processing all received messages
        time.sleep(45)
        # Once this sleep ends, it informs that it has finished publishing messages for this run, sending a None payload to the client done topic
        self.client.publish(client_done, None, qos=0)
        # Since this threaded function only runs during an execution, it returns after it's done, and the main thread waits for a new run order
        return

    # Cleanup function, to unsubscribe from the topics needed and gracefully close the connection to the broker
    def cleanup(self):
        self.client.unsubscribe(begin_client)
        self.client.disconnect()

    # Starts the class with all the variables necessary
    def __init__(self):
        self.msg_qos = 0
        self.msg_amount = 0
        self.msg_size = 0
        self.msg_freq = 0
        self.counter = 0
        logging.info(f"Creating MQTT Client with ID {client_id}")
        # Starts the MQTT client with specified ID, passed through the input arguments, and defines all callbacks
        self.client = mqtt.Client(client_id=client_id)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_publish = self.on_publish
        self.client.on_message = self.on_message
        # The MQTT client connects to the broker and the network loop iterates forever until the cleanup function
        self.client.connect(broker_address, 1883, 60)
        self.client.loop_forever()

# Starts one MQTT Client class object
mqtt_client = MQTT_Client()
