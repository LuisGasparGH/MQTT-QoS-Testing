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
log_folder = config['logging']['folder']
main_logger = config['logging']['main']
timestamp_logger = config['logging']['timestamp']
pyshark_logger = config['logging']['pyshark']
broker_address = config['broker_address']
main_topic = config['topics']['main_topic']
begin_client = config['topics']['begin_client']
finish_client = config['topics']['finish_client']
client_done = config['topics']['client_done']

# Class of the client code
class MQTT_Client:
    # Configures the logger which will contain all execution details for further analysis
    def logger_setup(self):
        formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
        main_log = log_folder + client_id + "-main.log"
        timestamp_log = log_folder + client_id + "-timestamp.log"
        pyshark_log = log_folder + client_id + "-pyshark.log"
        main_handler = logging.FileHandler(main_log, mode = 'a')
        timestamp_handler = logging.FileHandler(timestamp_log, mode = 'a')
        pyshark_handler = logging.FileHandler(pyshark_log, mode = 'a')
        main_handler.setFormatter(formatter)
        timestamp_handler.setFormatter(formatter)
        pyshark_handler.setFormatter(formatter)
        self.main_logger = logging.getLogger(main_logger)
        self.timestamp_logger = logging.getLogger(timestamp_logger)
        self.pyshark_logger = logging.getLogger(pyshark_logger)
        self.main_logger.setLevel(logging.INFO)
        self.timestamp_logger.setLevel(logging.INFO)
        self.pyshark_logger.setLevel(logging.INFO)
        self.main_logger.addHandler(main_handler)
        self.timestamp_logger.addHandler(timestamp_handler)
        self.pyshark_logger.addHandler(pyshark_handler)

    # Callback for when the client object successfully connects to the broker
    def on_connect(self, client, userdata, flags, rc):
        if rc==0:
            # Upon successful connection, the client subscribes to the start topic waiting for order
            self.main_logger.info(f"Connected to the broker at {broker_address}")
            self.client.subscribe(begin_client, qos=0)
            self.main_logger.info(f"Subscribed to {begin_client} topic with QoS 0")
            self.client.subscribe(finish_client, qos=0)
            self.main_logger.info(f"Subscribed to {finish_client} topic with QoS 0")
        else:
            # In case of error during connection the log will contain the error code for debugging
            self.main_logger.info(f"Error connecting to broker, with code {rc}")

    # Callback for when the client object receives a message, which includes topic and payload
    def on_message(self, client, userdata, msg):
        # In the topic of the message is the finish client, means the server has no more executions to order and is telling the clients to finish
        if str(msg.topic) == finish_client:
            self.cleanup()
        # If not, the topic will be the begin client, and the message will contain configuration information for the next run, such as message size, amount, publishing frequency and QoS level to be used
        elif str(msg.topic) == begin_client:
            self.main_logger.warning(f"STARTING NEW RUN")
            self.main_logger.info(f"Start order received from the server using topic {str(msg.topic)}")
            # Declares the thread where the run handler will run. Has to be done everytime a new run is executed
            self.handler_thread = threading.Thread(target = self.run_handler, args = ())
            # Parses the payload to a dictionary, and stores all the needed settings
            client_config = json.loads(msg.payload)
            self.msg_qos = client_config['msg_qos']
            self.msg_amount = client_config['msg_amount']
            self.msg_size = client_config['msg_size']
            self.msg_freq = client_config['msg_freq']
            self.sleep_time = (1/self.msg_freq)
            self.sent_counter = 0
            # Logs the run details and starts the run handler thread
            self.main_logger.info(f"Message details: {self.msg_amount} messages of {self.msg_size} bytes using QoS level {self.msg_qos} with {self.msg_freq} Hz frequency")
            self.handler_thread.start()

    # Callback for the when the client object successfully completes the publishing of a message (including necessary handshake for QoS levels 1 and 2)
    def on_publish(self, client, userdata, mid):
        # The client contains a counter of sent messages for logging purposes
        self.sent_counter += 1
        self.timestamp_logger.info(f"Published message #{self.sent_counter} to the {main_topic} topic (message id: {mid})")
        if self.sent_counter == self.msg_amount:
            self.main_logger.info(f"Publish of all {self.msg_amount} messages complete")

    # Callback for when the client object successfully disconnects from the broker
    def on_disconnect(self, client, userdata, rc):
        self.main_logger.info(f"Disconnected from broker at {broker_address}")

    # Run handler function, used to execute each run with the information received from the server
    def run_handler(self):
        # As per the requirements, the client sleeps for 2 seconds after it receives the order to start, and after that creates the payload with the specific size for the run
        time.sleep(2)
        payload = bytearray(self.msg_size)
        self.main_logger.info(f"Starting publish of {self.msg_amount} messages with QoS level {self.msg_qos}")
        # This cycle is iterated as many times as messages that need to be published in this run
        for msg in range(self.msg_amount):
            # Measures time of iteration start with a monotonic clock in order to precisely meet the frequency requirements
            time_start = time.monotonic()
            time_end = time_start+self.sleep_time
            # Messages are published with the correct QoS, and the thread sleeps for the necessary time to meet the frequency
            self.client.publish(main_topic, payload, qos=self.msg_qos)
            # Sleeps the thread for the remainder time of the current period in execution in order to meet the precise frequency
            if time.monotonic() < time_end:
                time.sleep(time_end - time.monotonic())
        # After all messages are sent, the client waits for a period of 45 seconds to make sure the server is finished processing all received messages
        time.sleep(10)
        # Once this sleep ends, it informs that it has finished publishing messages for this run, sending a None payload to the client done topic
        self.client.publish(client_done, None, qos=0)
        # Since this threaded function only runs during an execution, it returns after it's done, and the main thread waits for a new run order
        return

    # Cleanup function, to unsubscribe from the topics needed and gracefully close the connection to the broker
    def cleanup(self):
        self.client.unsubscribe(begin_client)
        self.client.unsubscribe(finish_client)
        self.client.disconnect()

    # Starts the class with all the variables necessary
    def __init__(self):
        self.logger_setup()
        self.main_logger.warning(f"NEW SYSTEM EXECUTION")
        self.msg_qos = 0
        self.msg_amount = 0
        self.msg_size = 0
        self.msg_freq = 0
        self.sent_counter = 0
        self.main_logger.info(f"Creating MQTT Client with ID {client_id}")
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
