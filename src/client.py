# Import of all necessary packages and libraries
import paho.mqtt.client as mqtt
import time
import json
import logging
import sys
import threading
import pyshark

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
wshark_folder = config['pyshark']['folder']
wshark_ext = config['pyshark']['extension']
wshark_interface = config['pyshark']['interface']

# Class of the client code
class MQTT_Client:
    # Configures the loggers which will contain all execution details for further analysis
    def logger_setup(self):
        # Sets up the formatter and handlers needed for the loggers
        # There are a total of three distinct loggers
        formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
        # main_logger - used for normal execution and results reporting
        main_log = log_folder + client_id + "-main.log"
        main_handler = logging.FileHandler(main_log, mode = 'a')
        main_handler.setFormatter(formatter)
        self.main_logger = logging.getLogger(main_logger)
        self.main_logger.setLevel(logging.DEBUG)
        self.main_logger.addHandler(main_handler)
        # timestamp_logger - used to have an output of all timestamps of messages sent/received
        timestamp_log = log_folder + client_id + "-timestamp.log"
        timestamp_handler = logging.FileHandler(timestamp_log, mode = 'a')
        timestamp_handler.setFormatter(formatter)
        self.timestamp_logger = logging.getLogger(timestamp_logger)
        self.timestamp_logger.setLevel(logging.DEBUG)
        self.timestamp_logger.addHandler(timestamp_handler)
        # pyshark_logger - used to have an output of all packets captured by PyShark
        pyshark_log = log_folder + client_id + "-pyshark.log"
        pyshark_handler = logging.FileHandler(pyshark_log, mode = 'a')
        pyshark_handler.setFormatter(formatter)
        self.pyshark_logger = logging.getLogger(pyshark_logger)
        self.pyshark_logger.setLevel(logging.DEBUG)
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
            self.main_logger.debug(f"STARTING NEW RUN")
            self.main_logger.info(f"Start order received from the server using topic {str(msg.topic)}")
            # Declares the thread where the run handler will run. Has to be done everytime a new run is executed
            self.handler_thread = threading.Thread(target = self.run_handler, args = ())
            # Declares the thread where PyShark will be sniffing the network traffic and capturing it to an appropriate file
            self.sniffing_thread = threading.Thread(target = self.sniffing_handler, args = ())
            # Parses the payload to a dictionary, and stores all the needed settings
            client_config = json.loads(msg.payload)
            self.msg_qos = client_config['msg_qos']
            self.msg_amount = client_config['msg_amount']
            self.msg_size = client_config['msg_size']
            self.msg_freq = client_config['msg_freq']
            self.sleep_time = (1/self.msg_freq)
            self.sent_counter = 0
            # Creates appropriate Wireshark file name for packet capture and analysis, and starts the wireshark sniffing thread
            self.wshark_file = wshark_folder + client_id + "-Q" + str(self.msg_qos) + "-A" + str(self.msg_amount) + "-S" + str(int(self.msg_size)) + "-F" + str(self.msg_freq) + "-T" + str(time.time_ns()) + str(wshark_ext)
            self.sniffing_thread.start()
            # Logs the run details and starts the run handler thread
            self.main_logger.info(f"Message amount: {self.msg_amount} messages")
            self.main_logger.info(f"Message size: {self.msg_size} bytes")
            self.main_logger.info(f"QoS level: {self.msg_qos}")
            self.main_logger.info(f"Publish frequency: {self.msg_freq} Hz")
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

    # Sniffing function, responsible for running PyShark and capturing all network traffic for further analysis in Wireshark
    def sniffing_handler(self):
        self.main_logger.info(f"Sniffing thread started for this run")
        # Starts a LiveCapture from PyShark on the correct interface, with filters applied to only capture MQTT or TCP port 1883 packets, and saves it to the previously named file
        self.network_capture = pyshark.LiveCapture(interface=wshark_interface, bpf_filter="tcp port 1883", output_file = self.wshark_file)
        for packet in self.network_capture.sniff_continuously():
            self.pyshark_logger.info(f"Packet captured: {packet}")
            # Since the sniffing has to be done once per run (every run has a different file), the sniffing thread stops when it detects an MQTT message to the client_done topic
            if hasattr(packet, "mqtt"):
                if packet.mqtt.mqtt.topic == client_done:
                    return
    
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
                time.sleep(time_end - time.monotonic() - 0.00008)
        # After all messages are sent, the client waits for a period of 45 seconds to make sure the server is finished processing all received messages
        self.main_logger.info(f"Sleeping for 10 seconds to allow for retransmission finishing")
        time.sleep(10)
        # Once this sleep ends, it informs that it has finished publishing messages for this run, sending a None payload to the client done topic
        self.client.publish(client_done, None, qos=0)
        self.main_logger.info(f"Informed server that client is finished")
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
        self.main_logger.debug(f"=============================================================================")
        self.timestamp_logger.debug(f"=============================================================================")
        self.pyshark_logger.debug(f"=============================================================================")
        self.main_logger.debug(f"NEW SYSTEM EXECUTION")
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
