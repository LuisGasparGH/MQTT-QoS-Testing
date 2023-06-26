# Import of all necessary packages and libraries
import paho.mqtt.client as mqtt
import time
import datetime
import json
import logging
import sys
import threading
import os
import uuid
import subprocess
import zipfile
import signal

# Reads the configuration file, and imports it into a dictionary, which includes information about:
# - Logging paths and names
# - MQTT topics
# - Execution system message details and repetitions
# - TShark details
with open("conf/config.json", "r") as config_file:
    config = json.load(config_file)

# Stores all static variables needed from the configuration dictionary, adapted to the server
# Also gathers the client-id from the input arguments, to be used in the MQTT client
client_id = "server"
log_folder = str(config['logging']['folder']).replace("#", client_id)
main_logger = config['logging']['main']
timestamp_logger = config['logging']['timestamp']
broker_address = config['broker_address']
main_topic = str(config['topics']['main_topic'])
begin_client = config['topics']['begin_client']
void_run = config['topics']['void_run']
finish_client = config['topics']['finish_client']
client_done = config['topics']['client_done']
system_runs = config['system_details']['different_runs']
run_repetitions = config['system_details']['run_repetitions']
message_details = config['system_details']['message_details']
wshark_folder = str(config['tshark']['folder']).replace("#", client_id)
wshark_filter = config['tshark']['filter']
wshark_ext = config['tshark']['extension']
wshark_interface = config['tshark']['interface']['server']
rtx_times = config['rtx_times']

# Class of the MQTT server code
class MQTT_Server:
    # Configures all the loggers to log and store every execution detail in appropriate files for future analysis
    # There are a total of two distinct loggers:
    # main_logger - used for all normal execution logging, regarding execution information and results reporting
    # timestamp_logger - used to have an output of all timestamps of messages received from the broker
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
            # Upon successful connection to the broker, the server does the following:
            # - defines a needed variable, to signal if it's connected (for the cleanup function)
            # - subscribes to the client_done topic
            # - starts the system handler thread
            self.mqtt_connected = True
            self.main_logger.info(f"Connected to the broker at {broker_address}")
            self.client.subscribe(client_done, qos=0)
            self.main_logger.info(f"Subscribed to {client_done} topic with QoS 0")
            self.client.subscribe(void_run, qos=0)
            self.main_logger.info(f"Subscribed to {void_run} topic with QoS 0")
            self.sys_thread.start()
        else:
            # In case of error during connection the log will contain the error code for debugging
            self.main_logger.info(f"Error connecting to broker, with code {rc}")

        # Callback for when the client object successfully disconnects from the broker
    
    # Callback for when the client object successfully disconnects from the broker
    def on_disconnect(self, client, userdata, rc):
        self.mqtt_connected = False
        if rc==0:
            self.main_logger.info(f"Disconnected from broker at {broker_address}")
        else:
            self.main_logger.warning(f"Abnormal disconnection from broker, with code {rc}")

    # Callback for when the server receives a message on the main topic, on any of the 10 clients
    # Its a callback per client instead of calculating the client on the received message, to try and minimize processing overhead during the transmission period
    def on_maintopic_c0(self, client, userdata, msg):
        # For every message received, increases the counter slot for the specific client, and logs the received datetime
        self.run_client_received[0] += 1
        self.run_client_timestamps[0].append(datetime.datetime.utcnow())
        self.timestamp_logger.info(f"Received message #{self.run_client_received[0]} from the {msg.topic} topic")
    
    def on_maintopic_c1(self, client, userdata, msg):
        self.run_client_received[1] += 1
        self.run_client_timestamps[1].append(datetime.datetime.utcnow())
        self.timestamp_logger.info(f"Received message #{self.run_client_received[1]} from the {msg.topic} topic")
    
    def on_maintopic_c2(self, client, userdata, msg):
        self.run_client_received[2] += 1
        self.run_client_timestamps[2].append(datetime.datetime.utcnow())
        self.timestamp_logger.info(f"Received message #{self.run_client_received[2]} from the {msg.topic} topic")
    
    def on_maintopic_c3(self, client, userdata, msg):
        self.run_client_received[3] += 1
        self.run_client_timestamps[3].append(datetime.datetime.utcnow())
        self.timestamp_logger.info(f"Received message #{self.run_client_received[3]} from the {msg.topic} topic")
    
    def on_maintopic_c4(self, client, userdata, msg):
        self.run_client_received[4] += 1
        self.run_client_timestamps[4].append(datetime.datetime.utcnow())
        self.timestamp_logger.info(f"Received message #{self.run_client_received[4]} from the {msg.topic} topic")
    
    def on_maintopic_c5(self, client, userdata, msg):
        self.run_client_received[5] += 1
        self.run_client_timestamps[5].append(datetime.datetime.utcnow())
        self.timestamp_logger.info(f"Received message #{self.run_client_received[5]} from the {msg.topic} topic")
    
    def on_maintopic_c6(self, client, userdata, msg):
        self.run_client_received[6] += 1
        self.run_client_timestamps[6].append(datetime.datetime.utcnow())
        self.timestamp_logger.info(f"Received message #{self.run_client_received[6]} from the {msg.topic} topic")
    
    def on_maintopic_c7(self, client, userdata, msg):
        self.run_client_received[7] += 1
        self.run_client_timestamps[7].append(datetime.datetime.utcnow())
        self.timestamp_logger.info(f"Received message #{self.run_client_received[7]} from the {msg.topic} topic")
    
    def on_maintopic_c8(self, client, userdata, msg):
        self.run_client_received[8] += 1
        self.run_client_timestamps[8].append(datetime.datetime.utcnow())
        self.timestamp_logger.info(f"Received message #{self.run_client_received[8]} from the {msg.topic} topic")
    
    def on_maintopic_c9(self, client, userdata, msg):
        self.run_client_received[9] += 1
        self.run_client_timestamps[9].append(datetime.datetime.utcnow())
        self.timestamp_logger.info(f"Received message #{self.run_client_received[9]} from the {msg.topic} topic")
    
    # Callback for when the server receives a message on the client done topic
    def on_clientdone(self, client, userdata, msg):
        # When a message in this topic is received, means a client has finished the publish and slept for the retransmission period
        # A counter of done clients is incremented, and when it reaches the amount of clients for the run, it is considered finished
        self.run_client_done += 1
        if self.run_client_done == self.run_client_amount:
            # When a run is finished, the server unsubscribes from the main topic, and changes a corresponding flag,
            # in order to proceed with result calculation and logging
            self.client.unsubscribe(main_topic)
            self.run_finished = True
    
    #Callback for when the server receives a message on the void run topic
    def on_voidrun(self, client, userdata, msg):
        # In case a client has a sudden reconnection to the broker, the run is void and repeated, in order to not halt progress
        self.main_logger.warning(f"{msg.payload.decode('utf-8')} reconnected to the broker, voiding current run when finished")
        self.void_run = True
    
    # Result logging function, used to calculate and output every relevant metric and result to the logger once a run is complete
    def result_logging(self):
        # Since the client message counters are in an array, a sum of all elements is needed to get the total message amount received
        run_msg_counter = sum(self.run_client_received)
        run_packet_loss = round(100-((run_msg_counter/self.run_total_msg_amount)*100),2)
        # The expected finish is the datetime start of the first received message for the client summed with the expected publish time
        client_expected_finish = [[] for _ in range(self.run_client_amount)]
        overall_start_time = None
        overall_finish_time = None
        # The run start and finish points are the datetime of the first received message overall and the last received message overall
        # To calculate so, since datetimes of the messages are per client, the server has to find the lowest datetime out of all first
        # elements of every timestamps array, and the highest datetime out of all elements of every timestamps array
        for client in range(self.run_client_amount):
            client_expected_finish[client] = min(self.run_client_timestamps[client]) + datetime.timedelta(seconds=self.run_expected_time)
            if overall_start_time == None:
                overall_start_time = min(self.run_client_timestamps[client])
            else:
                client_start_time = min(self.run_client_timestamps[client])
                if client_start_time < overall_start_time:
                    overall_start_time = client_start_time
            if overall_finish_time == None:
                overall_finish_time = max(self.run_client_timestamps[client])
            else:
                client_finish_time = max(self.run_client_timestamps[client])
                if client_finish_time > overall_finish_time:
                    overall_finish_time = client_finish_time
        # With the absolute start and finish, the other metrics are easily calculated and logged
        # These metrics include:
        # - Packet loss
        # - Run start, expected, and actual finish time
        # - Expected and actual publish time
        # - Perceived frequency from the server side
        # - Frequency and time factors compared to the perfect results
        run_exec_time = (overall_finish_time-overall_start_time)
        if run_exec_time.total_seconds() < self.run_expected_time:
            # TODO: Inform clients of void run in this situation
            self.main_logger.warning(f"Execution time is lower than expected time by {round(self.run_expected_time - run_exec_time.total_seconds(),3)} seconds, voiding current run")
            return False
        elif run_exec_time.total_seconds() >= self.run_expected_time:
            run_actual_freq = round((self.run_msg_amount-1)/(run_exec_time.total_seconds()),2)
            run_time_factor = round((run_exec_time.total_seconds()/self.run_expected_time),3)
            run_frequency_factor = round((run_actual_freq/self.run_msg_freq)*100,2)
            self.main_logger.info(f"All {self.run_client_amount} clients finished publishing for this execution")
            self.main_logger.info(f"==================================================")
            self.main_logger.info(f"RUN RESULTS")
            self.main_logger.info(f"Received {run_msg_counter} out of {self.run_total_msg_amount} messages")
            self.main_logger.info(f"Calculated packet loss: {run_packet_loss}%")
            self.main_logger.info(f"Run start time (of first received message): {overall_start_time.strftime('%H:%M:%S.%f')[:-3]}")
            self.main_logger.info(f"Run expected finish time: {min(client_expected_finish).strftime('%H:%M:%S.%f')[:-3]}")
            self.main_logger.info(f"Run actual finish time: {overall_finish_time.strftime('%H:%M:%S.%f')[:-3]}")
            self.main_logger.info(f"Expected execution time (for {self.run_msg_amount-1} messages): {round(self.run_expected_time,3)} seconds")
            self.main_logger.info(f"Total execution time (for {self.run_msg_amount-1} messages): {round(run_exec_time.total_seconds(),3)} seconds")
            self.main_logger.info(f"Time factor: {run_time_factor}x of the expected time")
            self.main_logger.info(f"Actual frequency: {run_actual_freq} Hz")
            self.main_logger.info(f"Frequency factor: {run_frequency_factor}%")
            return True

    # Sniffing function, responsible for executing the TShark commands and capturing all network traffic for further analysis in Wireshark
    def sniffing_handler(self):
        # Using the Subprocess module, starts a TShark capture with the following options:
        # - interface -> taken from the config file
        # - capture filter -> taken from the config file, should be "tcp port 1883" to only capture traffic on this port and protocol
        # - autostop condition -> since the sniffing is not meant to run indefinitely, but rather once per run, a timeout is indicated
        #                         That timeout is calculated using the predicted publish time (using message amount and frequency), and the retransmission sleep
        #                         time minus 10 seconds, so that it doesn't capture the client_done messages sent
        # - file -> defined before the thread was started, is the file name to which the capture will be output
        sniff_duration = (self.run_msg_amount/self.run_msg_freq)+rtx_times[self.run_msg_qos]+7.5
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

    # System handler function, used to iterate through the configuration runs and give orders to all clients with each run information
    def sys_handler(self):
        # Before the server starts ordering the runs, it will double check the config file to make 
        # sure all lists have the correct size
        # In case a parameter in the message details of the config is a simple int, it means that parameter is the same for all runs
        self.wrong_config = False
        for detail in message_details:
            if type(message_details[detail]) == list and len(message_details[detail]) != system_runs:
                self.wrong_config = True
                self.main_logger.warning(f"Problem in config file, {detail} has incorrect number of entries ({len(message_details[detail])}/{system_runs})")
        # In case any issue is found with the config file, performs cleanup and exits
        if self.wrong_config:
            self.cleanup()
        else:
            # The config file has a parameter with the amount of system runs to be performed, which will be iterated in here
            # However, to get a statistically relevant average, every different configuration is ran 10 times
            for run in range(system_runs):
                rep = 0
                # Generates an unique UUID for every different run, for easier identification in the logs
                self.run_uuid = str(uuid.uuid4())
                while rep < run_repetitions:
                    # Creates a unique UUID for the run, to allow for easier identification in the logs
                    # Indicates on the logger which run is currently being ran, for the user to keep track
                    self.main_logger.info(f"==================================================")
                    self.main_logger.info(f"EXECUTING RUN {run+1}/{system_runs} | REPETITION {rep+1}/{run_repetitions}")
                    self.main_logger.info(f"Run UUID: {self.run_uuid}")
                    self.timestamp_logger.info(f"==================================================")
                    self.timestamp_logger.info(f"EXECUTING RUN {run+1}/{system_runs} | REPETITION {rep+1}/{run_repetitions}")
                    self.timestamp_logger.info(f"Run UUID: {self.run_uuid}")
                    # Gathers all the information for the next run to be performed, such as:
                    # - client amount
                    # - QoS to be used
                    # - message amount
                    # - message payload size
                    # - publishing frequency
                    if type(message_details['client_amount']) == list:
                        self.run_client_amount = message_details['client_amount'][run]
                    else:
                        self.run_client_amount = message_details['client_amount']
                    if type(message_details['msg_qos']) == list:
                        self.run_msg_qos = message_details['msg_qos'][run]
                    else:
                        self.run_msg_qos = message_details['msg_qos']
                    if type(message_details['msg_amount']) == list:
                        self.run_msg_amount = message_details['msg_amount'][run]
                    else:
                        self.run_msg_amount = message_details['msg_amount']
                    if type(message_details['msg_size']) == list:
                        self.run_msg_size = message_details['msg_size'][run]
                    else:
                        self.run_msg_size = message_details['msg_size']
                    if type(message_details['msg_freq']) == list:
                        self.run_msg_freq = message_details['msg_freq'][run]
                    else:
                        self.run_msg_freq = message_details['msg_freq']
                    # Calculates the total expected messages as well as the theoretical execution time
                    self.run_total_msg_amount = self.run_msg_amount * self.run_client_amount
                    self.run_expected_time = (self.run_msg_amount-1) / self.run_msg_freq
                    # Creates the counter and timestamp arrays, with the same length as client amount in the run
                    self.run_client_received = [0 for _ in range(self.run_client_amount)]
                    self.run_client_timestamps = [[] for _ in range(self.run_client_amount)]
                    self.run_client_done = 0
                    self.void_run = False
                    os.makedirs(wshark_folder.replace("*C", f"{self.run_client_amount}C"), exist_ok=True)
                    self.basename = wshark_folder.replace("*C", f"{self.run_client_amount}C") + client_id + "-Q" + str(self.run_msg_qos) + "-A" + str(self.run_msg_amount) + \
                        "-S" + str(int(self.run_msg_size)) + "-F" + str(self.run_msg_freq)
                    # On the zip file, the run UUID and propper extension is added
                    self.zip_file =  self.basename + "-U" + self.run_uuid + ".zip"
                    # For the Wireshark file, an additional run repetition and timestamp string is added, like in the loggers, to differentiate between runs
                    # Files for runs with the exact same configuration (due to the fact that each configuration is ran multiple times to obtain an average) go into the same zip file
                    self.wshark_file = self.basename + "-R" + str(rep+1) + "-T" + str(datetime.datetime.utcnow().strftime('%d-%m-%Y_%H-%M-%S')) + wshark_ext
                    # Declares the thread where the sniffing handler function will be sniffing the network traffic. Has to be done everytime a new run is received
                    self.sniffing_thread = None
                    self.sniffing_thread = threading.Thread(target = self.sniffing_handler, args = ())
                    # Subscribes to the message topic with the correct QoS to be used in the run, and logs all the information of the run
                    self.client.subscribe(main_topic, qos=self.run_msg_qos)
                    self.main_logger.info(f"Subscribed to {main_topic} topic with QoS level {self.run_msg_qos}")
                    self.main_logger.info(f"Client amount: {self.run_client_amount} clients")
                    self.main_logger.info(f"Message amount per client: {self.run_msg_amount} messages")
                    self.main_logger.info(f"Total message amount: {self.run_total_msg_amount} messages")
                    self.main_logger.info(f"Message size: {self.run_msg_size} bytes")
                    self.main_logger.info(f"Publishing frequency: {self.run_msg_freq} Hz")
                    self.main_logger.info(f"QoS level: {self.run_msg_qos}")
                    # Dumps the information to a JSON payload to send to all the clients, and publishes it to the client topic
                    client_config = json.dumps({"uuid": str(self.run_uuid), "repetition": rep, "client_amount": self.run_client_amount, "msg_qos": self.run_msg_qos, "msg_amount": self.run_msg_amount, "msg_size": self.run_msg_size, "msg_freq": self.run_msg_freq})
                    self.client.publish(begin_client, client_config, qos=0)
                    self.main_logger.info(f"Sent configuration and start order to all the clients")
                    self.run_finished = False
                    self.sniffing_thread.start()
                    # While the run is not finished, the thread waits and periodically checks if the run has ended
                    while self.run_finished == False:
                        time.sleep(10)
                    os.kill(self.tshark_subprocess.pid, signal.SIGTERM)
                    if self.void_run == False:
                        # Once the run is ended, all results are calculated and logged
                        # In case the run is deemed invalid, the repetition counter is not incremented and the run is repeated once more
                        run_result = self.result_logging()
                        if run_result == True:
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
                            rep += 1
                    elif self.void_run == True:
                        self.main_logger.info(f"Deleting TShark capture file of current run due to being void")
                    os.remove(self.wshark_file)
            # Once all runs are finished, cleans up everything and exits
            self.cleanup()
    
    # Cleanup function, used to inform all clients to shutdown and gracefully clean everything MQTT related,
    # using the previously mentioned connected flag
    def cleanup(self):
        self.main_logger.info(f"==================================================")
        self.main_logger.info(f"Performing cleanup of MQTT connection, exiting and informing clients")
        # Removes all added callbacks, and in case the client is connected, publishes a client_done message with None payload
        # After that, unsubscribes from the topics, disconnects, and turns the flag to false
        for client in range(10):
            self.client.message_callback_remove(f"{main_topic}/client-{client}")
        self.client.message_callback_remove(client_done)
        if self.mqtt_connected:
            self.client.publish(finish_client, None, qos=0)
            self.client.unsubscribe(main_topic)
            self.client.unsubscribe(client_done)
            self.client.disconnect()

    # Starts the server class with all the variables necessary
    def __init__(self):
        # Creates the logs folder in case it doesn't exist
        os.makedirs(log_folder, exist_ok=True)
        # Performs the logger setup
        self.logger_setup()
        self.main_logger.info(f"==================================================")
        self.main_logger.info(f"NEW SYSTEM EXECUTION")
        # Declares the thread where the system handler will run. This only has to be done once per system execution
        self.sys_thread = threading.Thread(target = self.sys_handler, args=())
        self.main_logger.info(f"Creating MQTT Client with ID {client_id}")
        # Starts the MQTT client with specified ID, passed through the input arguments, and defines all callbacks
        self.mqtt_connected = False
        self.void_run = False
        self.client = mqtt.Client(client_id=client_id)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.message_callback_add(main_topic.replace("#", f"client-0"), self.on_maintopic_c0)
        self.client.message_callback_add(main_topic.replace("#", f"client-1"), self.on_maintopic_c1)
        self.client.message_callback_add(main_topic.replace("#", f"client-2"), self.on_maintopic_c2)
        self.client.message_callback_add(main_topic.replace("#", f"client-3"), self.on_maintopic_c3)
        self.client.message_callback_add(main_topic.replace("#", f"client-4"), self.on_maintopic_c4)
        self.client.message_callback_add(main_topic.replace("#", f"client-5"), self.on_maintopic_c5)
        self.client.message_callback_add(main_topic.replace("#", f"client-6"), self.on_maintopic_c6)
        self.client.message_callback_add(main_topic.replace("#", f"client-7"), self.on_maintopic_c7)
        self.client.message_callback_add(main_topic.replace("#", f"client-8"), self.on_maintopic_c8)
        self.client.message_callback_add(main_topic.replace("#", f"client-9"), self.on_maintopic_c9)
        self.client.message_callback_add(client_done, self.on_clientdone)
        self.client.message_callback_add(void_run, self.on_voidrun)
        # The MQTT client connects to the broker and the network loop iterates forever until the cleanup function
        # The keep alive is set to 3 hours
        self.client.connect(broker_address, 1883, 60)
        self.client.loop_forever()

# Starts one MQTT Server class object
# Small exception handler in case the user decides to use Ctrl-C to finish the program mid execution
try:
    mqtt_server = MQTT_Server()
except KeyboardInterrupt:
    print("Detected user interruption, shutting down...")
