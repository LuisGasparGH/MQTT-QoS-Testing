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
import zipfile

# Reads the configuration file, and imports it into a dictionary, which includes information about:
# - Logging paths and names
# - MQTT topics
# - Execution system message details and repetitions
# - TShark details
with open("conf/config.json", "r") as config_file:
    config = json.load(config_file)

# Stores all static variables needed from the configuration dictionary, adapted to the client
# Also gathers the client-id from the input arguments, to be used in the MQTT client
client_number = int(sys.argv[1])
client_id = "client-"+ str(client_number)
log_folder = str(config['logging']['folder']).replace("#", client_id)
main_logger = config['logging']['main']
timestamp_logger = config['logging']['timestamp']
broker_address = config['broker_address']
main_topic = str(config['topics']['main_topic']).replace("#", client_id)
begin_client = config['topics']['begin_client']
void_run = config['topics']['void_run']
finish_client = config['topics']['finish_client']
client_done = config['topics']['client_done']
wshark_folder = str(config['tshark']['folder']).replace("#", client_id)
wshark_filter = config['tshark']['filter']
wshark_ext = config['tshark']['extension']
wshark_interface = config['tshark']['interface']['client']
rtx_times = config['rtx_times']

# Class of the MQTT client code
class MQTT_Client:
    # Configures all the loggers to log and store every execution detail in appropriate files for future analysis
    # There are a total of two distinct loggers:
    # main_logger - used for all normal execution logging, regarding execution information and results reporting
    # timestamp_logger - used to have an output of all timestamps of messages published to the broker
    def logger_setup(self):
        # Gathers current GMT/UTC datetime in string format, to append to the logger file name
        # This will allow distinction between different runs, as well as make it easy to locate the parity between client and server
        # logs, as the datetime obtained on both will be identical
        append_time = datetime.datetime.utcnow().strftime('%d-%m-%Y_%H-%M-%S')
        # Setup of the formatter for the loggers, to display time, levelname and message, and converts logger timezone to GMT as well
        formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')
        formatter.converter = time.gmtime
        # Setup of the file handler, to output the logging into the propperly named files
        main_log = log_folder + client_id + "-main-T" + str(append_time) + ".log"
        main_handler = logging.FileHandler(main_log, mode = 'a')
        main_handler.setFormatter(formatter)
        self.main_logger = logging.getLogger(main_logger)
        self.main_logger.setLevel(logging.DEBUG)
        self.main_logger.addHandler(main_handler)
        timestamp_log = log_folder + client_id + "-timestamp-T" + str(append_time) + ".log"
        timestamp_handler = logging.FileHandler(timestamp_log, mode = 'a')
        timestamp_handler.setFormatter(formatter)
        self.timestamp_logger = logging.getLogger(timestamp_logger)
        self.timestamp_logger.setLevel(logging.DEBUG)
        self.timestamp_logger.addHandler(timestamp_handler)
        # An additional handler is added, for terminal output of the main logger, along with the file output
        # This is useful for the user to keep track of the execution status of a run without having to open the log files (very useful in SSH)
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(formatter)
        self.main_logger.addHandler(stdout_handler)
        # self.timestamp_logger.addHandler(stdout_handler)

    # Callback for when the client object successfully connects to the broker with specified address
    def on_connect(self, client, userdata, flags, rc):
        if rc==0:
            # Upon successful connection to the broker, the client does the following:
            # - defines too needed variables, for the zip file of the network capture files, and to signal if it's connected (for the cleanup)
            # - subscribes to the begin_client and finish_client topics, used to receive orders from the server
            self.main_logger.info(f"Connected to the broker at {broker_address}")
            self.mqtt_connected = True
            self.connect_count += 1
            self.zip = None
            self.client.subscribe(begin_client, qos=0)
            self.main_logger.info(f"Subscribed to {begin_client} topic with QoS 0")
            self.client.subscribe(finish_client, qos=0)
            self.main_logger.info(f"Subscribed to {finish_client} topic with QoS 0")
            self.client.subscribe(void_run, qos=0)
            self.main_logger.info(f"Subscribed to {void_run} topic with QoS 0")
            if self.connect_count > 1:
                self.main_logger.warning(f"Client reconnected to broker, telling server to void current run")
                self.client.publish(void_run, payload=client_id, qos=0)
                self.void_run = True
        else:
            # In case of error during connection the log will contain the error code for debugging
            self.main_logger.warning(f"Error connecting to broker, with code {rc}")

    # Callback for when the client object successfully disconnects from the broker
    def on_disconnect(self, client, userdata, rc):
        self.mqtt_connected = False
        if rc==0:
            self.main_logger.info(f"Disconnected from broker at {broker_address}")
        else:
            self.main_logger.warning(f"Abnormal disconnection from broker, with code {rc}")

    # Callback for the when the client object successfully completes the publish of a message (including necessary handshake for QoS levels 1 and 2)
    def on_publish(self, client, userdata, mid):
        # For every run, the client contains an internal counter of published messages, used for internal measurements
        self.sent_counter += 1
        # Upon the first message published in each run, the client measures datetime at that point
        if self.sent_counter == 1:
            self.publish_begin = datetime.datetime.now()
        self.timestamp_logger.info(f"Published message #{self.sent_counter} to the {main_topic} topic")
        # When all messages are published to the broker, a the final datetime is measured, in order to have a client-side publish time
        # Changes the pub_complete flag to true in order to make the client proceed
        if self.sent_counter == self.msg_amount:
            self.main_logger.info(f"Publish of all {self.msg_amount} messages complete")
            self.publish_end = datetime.datetime.now()
            self.pub_complete = True

    # Callback for when the client receives a message on the topic begin client
    def on_beginclient(self, client, userdata, msg):
        self.main_logger.info(f"==================================================")
        self.main_logger.info(f"Start order received from the server using topic {str(msg.topic)}")
        # The run configuration is sent by the server via the begin_client messages
        # Once a message on this topic is received, the payload is parsed to a dictionary
        client_config = json.loads(msg.payload)
        # In order to help with run automation, the clients use one variable from the run configuration to check if they will be used for the received run
        # That variable is the client_amount, which indicates how many clients are used to this run
        # Since all clients have the same nomenclature (client-X), and start from 0, a quick string comparison can be used to check if the client will be used or not
        self.main_logger.info(f"Verifying if {client_id} will be used for this run")
        client_amount = int(client_config['client_amount'])
        if client_number >= client_amount:
            # This client is not going to be used for this run, skipping
            self.main_logger.info(f"{client_id} will not be used for this run, skipping and waiting for next start order")
        elif client_number < client_amount:
            # This client is going to be used for this run, proceeding as normal
            # Every run has a unique UUID for easier identification in the logs. Every repetition has the same UUID
            self.run_uuid = client_config['uuid']
            self.main_logger.info(f"==================================================")
            self.main_logger.info(f"STARTING NEW RUN")
            self.main_logger.info(f"Run UUID: {self.run_uuid}")
            self.timestamp_logger.info(f"==================================================")
            self.timestamp_logger.info(f"STARTING NEW RUN")
            self.timestamp_logger.info(f"Run UUID: {self.run_uuid}")
            # Declares the thread where the run handler function will run. Has to be done everytime a new run is received
            self.run_thread = None
            self.run_thread = threading.Thread(target = self.run_handler, args = ())
            # Stores all needed run message settings, as well as calculates sleep periods for normal publish
            self.run_repetition = client_config['repetition']
            self.msg_qos = client_config['msg_qos']
            self.msg_amount = client_config['msg_amount']
            self.msg_size = client_config['msg_size']
            self.msg_freq = client_config['msg_freq']
            self.sleep_time = (1000/self.msg_freq)+0.01
            self.rtx_sleep = rtx_times[self.msg_qos]
            self.sent_counter = 0
            self.void_run = False
            # Every run generates a Wireshark capture file, that is then compressed to a zip file with similar name
            # For that effect, a base common name is defined, and then the specifics for each different file are added later (such as extensions)
            # The base name has a specific pattern: wireshark/client-X/*C/client-X-QX-AX-SX-FX
            # - *C -> indicates the amount of clients used for this run (only relevant for storage organization purposes)
            # - client-X -> indicates the client which generated this file
            # - QX -> indicates the QoS used for this run
            # - AX -> indicates the message amount published in this run
            # - SX -> indicates the payload size of each message published for this run
            # - FX -> indicates the publish frequency used for this run
            # Creates the logs folder in case it doesn't exist
            os.makedirs(wshark_folder.replace("*C", f"{client_amount}C"), exist_ok=True)
            self.basename = wshark_folder.replace("*C", f"{client_amount}C") + client_id + "-Q" + str(self.msg_qos) + "-A" + str(self.msg_amount) + \
                "-S" + str(int(self.msg_size)) + "-F" + str(self.msg_freq)
            # On the zip file, the run UUID and propper extension is added
            self.zip_file =  self.basename + "-U" + self.run_uuid + ".zip"
            # For the Wireshark file, an additional run repetition and timestamp string is added, like in the loggers, to differentiate between runs
            # Files for runs with the exact same configuration (due to the fact that each configuration is ran multiple times to obtain an average) go into the same zip file
            self.wshark_file = self.basename + "-R" + str(self.run_repetition+1) + "-T" + str(datetime.datetime.utcnow().strftime('%d-%m-%Y_%H-%M-%S')) + wshark_ext
            # Declares the thread where the sniffing handler function will be sniffing the network traffic. Has to be done everytime a new run is received
            self.sniffing_thread = None
            self.sniffing_thread = threading.Thread(target = self.sniffing_handler, args = ())
            # Logs the run message details and starts both previous handler threads
            self.main_logger.info(f"Message amount: {self.msg_amount} messages")
            self.main_logger.info(f"Message size: {self.msg_size} bytes")
            self.main_logger.info(f"QoS level: {self.msg_qos}")
            self.main_logger.info(f"Publish frequency: {self.msg_freq} Hz")
            self.sniffing_thread.start()
            self.run_thread.start()

    # Callback for when the client receives a message on the topic finish client
    def on_finishclient(self, client, userdata, msg):
        self.main_logger.info(f"==================================================")
        self.main_logger.info(f"End order received from the server using topic {str(msg.topic)}")
        self.cleanup()

    # Callback for when the client receives a message on the void run topic
    def on_voidrun(self, client, userdata, msg):
        # When a run is void, the other clients receive that indication as well, to ignore the results and delete the capture file
        self.main_logger.warning(f"One of the clients has reconnected to the broker, voiding current run when finished")
        self.void_run = True
    # Cleanup function, used to gracefully clean everything MQTT related, using the previously mentioned connected flag
    def cleanup(self):
        # Removes all added callbacks, and in case the client is connected, unsubscribes from the topics and disconnects
        # After, turns the flag to false
        self.main_logger.info(f"Cleaning up MQTT connection and exiting")
        self.client.message_callback_remove(begin_client)
        self.client.message_callback_remove(finish_client)
        if self.mqtt_connected:
            self.client.unsubscribe(begin_client)
            self.client.unsubscribe(finish_client)
            self.client.disconnect()

    # Sniffing function, responsible for executing the TShark commands and capturing all network traffic for further analysis in Wireshark
    def sniffing_handler(self):
        # Using the Subprocess module, starts a TShark capture with the following options:
        # - interface -> taken from the config file, usually eth0
        # - capture filter -> taken from the config file, should be "tcp port 1883" to only capture traffic on this port and protocol
        # - autostop condition -> since the sniffing is not meant to run indefinitely, but rather once per run, a timeout is indicated
        #                         That timeout is calculated using the predicted publish time (using message amount and frequency), and the retransmission sleep
        #                         time minus 10 seconds, so that it doesn't capture the client_done messages sent
        # - file -> defined before the thread was started, is the file name to which the capture will be output
        sniff_duration = (self.msg_amount/self.msg_freq)+self.rtx_sleep
        self.main_logger.info(f"Sniffing thread started")
        self.main_logger.info(f"Setting up TShark subprocess capture")
        self.main_logger.info(f"Interface: {wshark_interface}")
        self.main_logger.info(f"Capture Filter: {wshark_filter}")
        self.main_logger.info(f"Capture file: {os.path.basename(self.wshark_file)}")
        self.main_logger.info(f"Sniffing duration: {round(sniff_duration,2)} seconds")
        tshark_call = ["tshark", "-i", wshark_interface, "-f", wshark_filter,
                       "-a", f"duration:{sniff_duration}", "-w", self.wshark_file]
        # Starts the subprocess and saves the details (including process ID), but directs any output to DEVNULL in order to keep the logs clean
        self.tshark_subprocess = subprocess.Popen(tshark_call, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    # Run handler function, used to execute each run with the information received from the server
    def run_handler(self):
        self.main_logger.info(f"Run thread started")
        # In order to give the client some setup time for the TShark capture, the client sleeps for 5 seconds before starting the run
        time.sleep(5)
        # Creates a payload with the appropriate size, and defines some variables, more specifically the publish_begin and publish_end
        # These is where the datetimes from the client-side publish measurement will be stored
        payload = bytearray(self.msg_size)
        self.pub_complete = False
        self.publish_begin = None
        self.publish_end = None
        self.main_logger.info(f"Starting publish of {self.msg_amount} messages with QoS level {self.msg_qos}")
        # Since there is a specific publish frequency to be met, the publish function may need to sleep between publishes, using the beforementioned sleep period
        # However, for better precision, instead of calculating the remaining time left for the function to sleep, the pause library is used
        # This library works with datetime objects. In order to pause the function, before every execution, a deadline is calculated
        # This deadline is increased with the sleep period everytime, and indicates until when the function must pause, with microsecond precision
        deadline = datetime.datetime.now()
        # A cycle is iterated as many times as messages that need to be published in this run
        for msg in range(self.msg_amount):
            # Updates the deadline of this iteration start with a current datetime object
            deadline += datetime.timedelta(milliseconds=(self.sleep_time))
            # MQTT client publishes the messages to the main topic, with the built payload and correct QoS
            self.client.publish(main_topic, payload, qos=self.msg_qos)
            # Pauses the thread until the deadline specified is met
            pause.until(deadline)
        # Once the iteration is complete, simply waits for MQTT client that all messages have been sent, before proceeding to the next step
        while (self.pub_complete != True) and (self.void_run != True):
            time.sleep(2.5)
        # After all messages are sent, the client logs the total publish time from the client side, but for the amount of messages minus 1, to compare correctly
        # with the server logs and determine if any delays happened and where
        if self.void_run == False:
            pub_time = self.publish_end-self.publish_begin
            pub_freq = round((self.sent_counter-1)/pub_time.total_seconds(),2)
            self.main_logger.info(f"Publishing started: {self.publish_begin.strftime('%H:%M:%S.%f')[:-3]}")
            self.main_logger.info(f"Publishing ended: {self.publish_end.strftime('%H:%M:%S.%f')[:-3]}")
            self.main_logger.info(f"Total publish time (for {self.msg_amount-1} messages): {round(pub_time.total_seconds(),3)} seconds")
            self.main_logger.info(f"Actual frequency (from the client): {pub_freq} Hz")
        # In order to allow for any needed retransmission of the messages from the broker to the server, the thread sleeps for a specific period of time,
        # which depends on QoS and is determined in the configuration file
        self.main_logger.info(f"Sleeping for {self.rtx_sleep} seconds to allow for retransmission finishing for QoS {self.msg_qos}")
        time.sleep(self.rtx_sleep)
        # After this sleep, just to make sure it has terminated, the client sends a SIGKILL signal to the sniffing subprocess
        os.kill(self.tshark_subprocess.pid, signal.SIGTERM)
        if self.void_run == False:
            # Due to memory caching performed by TShark, memory usage can grow very quickly when sniffing the network, which would cause MQTT connection issues
            # in long runs, voiding the results
            # To avoid this, as soon as a run is complete, the capture file is compressed into the previously mentioned zip file
            # Once zipped, the original files are deleted, to free up the cached memory as well as storage space
            self.main_logger.info(f"Zipping TShark capture files to free up memory")
            self.main_logger.info(f"Zip file: {os.path.basename(self.zip_file)}")
            self.zip = zipfile.ZipFile(self.zip_file, "a", zipfile.ZIP_DEFLATED)
            self.main_logger.info(f"Zipping and deleting {os.path.basename(self.wshark_file)}")
            self.zip.write(self.wshark_file, os.path.basename(self.wshark_file))
            self.zip.close()
        elif self.void_run == True:
            self.main_logger.info(f"Deleting TShark capture file of current run due to being void")
        os.remove(self.wshark_file)
        # At least, the client has to inform the server that it has finished publishing messages for this run
        # This is done by sending a None payload to the client done topic
        self.client.publish(client_done, None, qos=0)
        self.main_logger.info(f"Informed server that client is finished")

    # Starts the client class with all the variables necessary
    def __init__(self):
        # Creates the logs folder in case it doesn't exist
        os.makedirs(log_folder, exist_ok=True)
        # Performs the logger setup
        self.logger_setup()
        self.main_logger.info(f"==================================================")
        self.main_logger.info(f"NEW SYSTEM EXECUTION")
        self.main_logger.info(f"Creating MQTT Client with ID {client_id}")
        # Starts the MQTT client with specified client ID, passed through the input arguments, and defines all callbacks
        self.mqtt_connected = False
        self.connect_count = 0
        self.client = mqtt.Client(client_id=client_id)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_publish = self.on_publish
        self.client.message_callback_add(begin_client, self.on_beginclient)
        self.client.message_callback_add(finish_client, self.on_finishclient)
        self.client.message_callback_add(void_run, self.on_voidrun)
        # The MQTT client connects to the broker and the network loop iterates forever until the cleanup function
        # The keepalive is set to 3 hours, to try and avoid the ping messages to appear on the capture files
        self.client.connect(broker_address, 1883, 60)
        self.client.loop_forever()

# Starts one MQTT Client class object
# Small exception handler in case the user decides to use Ctrl-C to finish the program mid execution
try:
    mqtt_client = MQTT_Client()
except KeyboardInterrupt:
    print("Detected user interruption, shutting down...")
