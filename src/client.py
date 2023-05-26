# Import of all necessary packages and libraries
import paho.mqtt.client as mqtt
import time
import datetime
import json
import logging
import sys
import threading
import os
import pause
import subprocess
import signal

# Read the configuration file, which includes information about MQTT topics, various file paths, etc
with open("conf/config.json", "r") as config_file:
    config = json.load(config_file)

# Stores all static variables needed from the read configuration file, as well as the client-id from the input arguments
client_id = str(sys.argv[1])
log_folder = str(config['logging']['folder']).replace("client-#", client_id)
main_logger = config['logging']['main']
timestamp_logger = config['logging']['timestamp']
broker_address = config['broker_address']
main_topic = str(config['topics']['main_topic']).replace("#", client_id)
begin_client = config['topics']['begin_client']
finish_client = config['topics']['finish_client']
client_done = config['topics']['client_done']
wshark_folder = str(config['tshark']['folder']).replace("client-#", client_id)
wshark_filter = config['tshark']['filter']
wshark_ext = config['tshark']['extension']
wshark_interface = config['tshark']['interface']
rtx_times = config['rtx_times']

# Class of the MQTT client code
class MQTT_Client:
    # Configures all loggers which will contain every execution detail for further analysis
    def logger_setup(self):
        # Gathers current time in string format to append to the logger file name, to allow distinction between different runs
        append_time = datetime.datetime.utcnow().strftime('%H-%M-%S')
        # Sets up the formatter and handlers needed for the loggers
        formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')
        formatter.converter = time.gmtime
        # There are a total of two distinct loggers
        # main_logger - used for normal execution and information and results reporting
        main_log = log_folder + client_id + "-main-" + str(append_time) + ".log"
        main_handler = logging.FileHandler(main_log, mode = 'a')
        main_handler.setFormatter(formatter)
        self.main_logger = logging.getLogger(main_logger)
        self.main_logger.setLevel(logging.DEBUG)
        self.main_logger.addHandler(main_handler)
        # timestamp_logger - used to have an output of all timestamps of messages sent/received
        timestamp_log = log_folder + client_id + "-timestamp-" + str(append_time) + ".log"
        timestamp_handler = logging.FileHandler(timestamp_log, mode = 'a')
        timestamp_handler.setFormatter(formatter)
        self.timestamp_logger = logging.getLogger(timestamp_logger)
        self.timestamp_logger.setLevel(logging.DEBUG)
        self.timestamp_logger.addHandler(timestamp_handler)
        # Console output of the main logger for the user to keep track of the execution status without having to open the logs
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(formatter)
        self.main_logger.addHandler(stdout_handler)
        # self.timestamp_logger.addHandler(stdout_handler)

    # Callback for when the client object successfully connects to the broker
    def on_connect(self, client, userdata, flags, rc):
        if rc==0:
            # Upon successful connection, the client subscribes to the begin and finish topics, used to receive orders from the server
            self.mqtt_connected = True
            self.main_logger.info(f"Connected to the broker at {broker_address}")
            self.client.subscribe(begin_client, qos=0)
            self.main_logger.info(f"Subscribed to {begin_client} topic with QoS 0")
            self.client.subscribe(finish_client, qos=0)
            self.main_logger.info(f"Subscribed to {finish_client} topic with QoS 0")
        else:
            # In case of error during connection the log will contain the error code for debugging
            self.main_logger.warning(f"Error connecting to broker, with code {rc}")

    # Callback for when the client object successfully disconnects from the broker
    def on_disconnect(self, client, userdata, rc):
        self.main_logger.info(f"Disconnected from broker at {broker_address}")

    # Callback for the when the client object successfully completes the publishing of a message (including necessary handshake for QoS levels 1 and 2)
    def on_publish(self, client, userdata, mid):
        # The client contains a counter of sent messages for logging purposes
        self.sent_counter += 1
        # Measures datetime of first message published
        if self.sent_counter == 1:
            self.publish_begin = datetime.datetime.now()
        self.timestamp_logger.info(f"Published message #{self.sent_counter} to the {main_topic} topic")
        # When all messages are sent to the broker, that information is written on the logs, as well as the final message datetime
        if self.sent_counter == self.msg_amount:
            self.main_logger.info(f"Publish of all {self.msg_amount} messages complete")
            self.publish_end = datetime.datetime.now()
            self.pub_complete = True

    # Callback for when the client receives a message on the topic begin client
    def on_beginclient(self, client, userdata, msg):
        self.main_logger.info(f"==================================================")
        self.main_logger.info(f"Start order received from the server using topic {str(msg.topic)}")
        # Parses the payload to a dictionary
        client_config = json.loads(msg.payload)
        # To aid with run automation, the clients use a parameter from the payload received to check if they will be used for the received run
        client_check = "client-" + str(client_config['client_amount'])
        self.main_logger.info(f"Verifying if {client_id} will be used for this run")
        if client_id >= client_check:
            # This client is not going to be used for this run, skipping
            self.main_logger.info(f"{client_id} will not be used for this run, skipping and waiting for next start order")
        elif client_id < client_check:
            self.main_logger.info(f"==================================================")
            self.main_logger.info(f"STARTING NEW RUN")
            self.timestamp_logger.info(f"==================================================")
            self.timestamp_logger.info(f"STARTING NEW RUN")
            # This client is going to be used for this run, proceeding as normal
            # Declares the thread where the run handler will run. Has to be done everytime a new run is executed
            self.run_thread = None
            self.run_thread = threading.Thread(target = self.run_handler, args = ())
            # Stores all needed run settings
            self.msg_qos = client_config['msg_qos']
            self.msg_amount = client_config['msg_amount']
            self.msg_size = client_config['msg_size']
            self.msg_freq = client_config['msg_freq']
            self.sleep_time = (1/self.msg_freq)
            self.sent_counter = 0
            self.rtx_sleep = rtx_times[self.msg_qos]
            # Creates appropriate Wireshark capture file name for packet capture and analysis, and starts the sniffing thread
            wshark_file = wshark_folder + client_id + "-Q" + str(self.msg_qos) + "-A" + str(self.msg_amount) + "-S" + str(int(self.msg_size)) + \
                "-F" + str(self.msg_freq) +"-T" + str(datetime.datetime.utcnow().strftime('%H-%M-%S')) + str(wshark_ext)
            # Declares the thread where PyShark will be sniffing the network traffic and capturing it to an appropriate file
            self.sniffing_thread = None
            self.sniffing_thread = threading.Thread(target = self.sniffing_handler, args = (wshark_file,))
            # Logs the run details and starts the run handler thread
            self.main_logger.info(f"Message amount: {self.msg_amount} messages")
            self.main_logger.info(f"Message size: {self.msg_size} bytes")
            self.main_logger.info(f"QoS level: {self.msg_qos}")
            self.main_logger.info(f"Publish frequency: {self.msg_freq} Hz")
            self.sniffing_thread.start()
            self.run_thread.start()

    # Callback for when the client receives a message on the topic finish client
    # Performs the cleanup, to unsubscribe from the topics needed and gracefully close the connection to the broker, and also stops the PyShark capture
    def on_finishclient(self, client, userdata, msg):
        self.main_logger.info(f"==================================================")
        self.main_logger.info(f"End order received from the server using topic {str(msg.topic)}")
        self.cleanup()

    def cleanup(self):
        self.main_logger.info(f"Cleaning up MQTT connection and exiting")
        self.client.message_callback_remove(begin_client)
        self.client.message_callback_remove(finish_client)
        if self.mqtt_connected:
            self.client.unsubscribe(begin_client)
            self.client.unsubscribe(finish_client)
            self.client.disconnect()

    # Sniffing function, responsible for running PyShark and capturing all network traffic for further analysis in Wireshark
    def sniffing_handler(self, wshark_file):
        # Starts a LiveCapture from TShark directly on the correct interface, with filters applied to only capture TCP port 1883 packets,
        # to the specific capture file, and with a maximum duration
        sniff_duration = (self.msg_amount/self.msg_freq)+self.rtx_sleep-10
        self.main_logger.info(f"Sniffing thread started")
        self.main_logger.info(f"Setting up TShark subprocess capture")
        self.main_logger.info(f"Interface: {wshark_interface}")
        self.main_logger.info(f"BPF Filter: {wshark_filter}")
        self.main_logger.info(f"Capture file: {wshark_file}")
        self.main_logger.info(f"Sniffing duration: {round(sniff_duration,2)} seconds")
        tshark_call = ["tshark", "-i", wshark_interface, "-f", wshark_filter, "-a", f"duration:{sniff_duration}", "-w", wshark_file]
        self.tshark_subprocess = subprocess.Popen(tshark_call, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    # Run handler function, used to execute each run with the information received from the server
    def run_handler(self):
        self.main_logger.info(f"Run thread started")
        # As per the requirements, the client sleeps for 5 seconds to allow for initial setups (packet sniffing) after it receives the order to start,
        # and after that creates the payload withthe specific size for the run
        time.sleep(5)
        payload = bytearray(self.msg_size)
        self.pub_complete = False
        self.publish_begin = None
        self.publish_end = None
        self.main_logger.info(f"Starting publish of {self.msg_amount} messages with QoS level {self.msg_qos}")
        # This cycle is iterated as many times as messages that need to be published in this run
        deadline = datetime.datetime.now()
        for msg in range(self.msg_amount):
            # Measures time of iteration start with a current datetime object in order to precisely meet the frequency requirements
            deadline += datetime.timedelta(seconds=(self.sleep_time))
            # Messages are published with the correct QoS, and the thread sleeps for the necessary time to meet the frequency
            self.client.publish(main_topic, payload, qos=self.msg_qos)
            # Pauses the thread for the remainder time of the current period in execution in order to meet the precise frequency
            pause.until(deadline)
        # After all messages are sent, the client logs publishing time taken data, from the client side
        # After that, it waits for a certain period, depending on QoS level, to make sure the server is finished processing all received messages
        while self.pub_complete != True:
            pass
        pub_time = self.publish_end-self.publish_begin
        pub_freq = round((self.sent_counter-1)/pub_time.total_seconds(),2)
        self.main_logger.info(f"Publishing started: {self.publish_begin.strftime('%H:%M:%S.%f')[:-3]}")
        self.main_logger.info(f"Publishing ended: {self.publish_end.strftime('%H:%M:%S.%f')[:-3]}")
        self.main_logger.info(f"Total publish time (for {self.msg_amount-1} messages): {round(pub_time.total_seconds(),3)} seconds")
        self.main_logger.info(f"Actual frequency (from the client): {pub_freq} Hz")
        self.main_logger.info(f"Sleeping for {self.rtx_sleep} seconds to allow for retransmission finishing for QoS {self.msg_qos}")
        time.sleep(self.rtx_sleep)
        os.kill(self.tshark_subprocess.pid, signal.SIGTERM)
        # Once this sleep ends, it informs that it has finished publishing messages for this run, sending a None payload to the client done topic
        self.client.publish(client_done, None, qos=0)
        self.main_logger.info(f"Informed server that client is finished")

    # Starts the class with all the variables necessary
    def __init__(self):
        # Creates the logs and wireshark folders in case they doesn't exist
        os.makedirs(log_folder, exist_ok=True)
        os.makedirs(wshark_folder, exist_ok=True)
        self.logger_setup()
        self.main_logger.info(f"==================================================")
        self.main_logger.info(f"NEW SYSTEM EXECUTION")
        self.main_logger.info(f"Creating MQTT Client with ID {client_id}")
        # Starts the MQTT client with specified ID, passed through the input arguments, and defines all callbacks
        self.mqtt_connected = False
        self.client = mqtt.Client(client_id=client_id)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_publish = self.on_publish
        self.client.message_callback_add(begin_client, self.on_beginclient)
        self.client.message_callback_add(finish_client, self.on_finishclient)
        # The MQTT client connects to the broker and the network loop iterates forever until the cleanup function
        self.client.connect(broker_address, 1883, 60)
        self.client.loop_forever()

# Starts one MQTT Client class object
# Small exception handler in case the user decides to use Ctrl-C to finish the program mid execution
try:
    mqtt_client = MQTT_Client()
except KeyboardInterrupt:
    print("Detected user interruption, shutting down...")
