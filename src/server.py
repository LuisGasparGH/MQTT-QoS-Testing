# Import of all necessary packages and libraries
import paho.mqtt.client as mqtt
import time
import datetime
import json
import logging
import sys
import threading
import os

# Read the configuration file, which includes information about MQTT topics, various file paths, etc
with open("conf/config.json", "r") as config_file:
    config = json.load(config_file)

# Stores all static variables needed from the read configuration file, as well as the client-id from the input arguments
client_id = str(sys.argv[1])
log_folder = str(config['logging']['folder']).replace("client-#", client_id)
main_logger = config['logging']['main']
timestamp_logger = config['logging']['timestamp']
broker_address = config['broker_address']
main_topic = str(config['topics']['main_topic'])
begin_client = config['topics']['begin_client']
finish_client = config['topics']['finish_client']
client_done = config['topics']['client_done']
system_runs = config['system_details']['different_runs']
message_details = config['system_details']['message_details']

# Class of the MQTT server code
class MQTT_Server:
    # Configures all loggers which will contain every execution detail for further analysis
    def logger_setup(self):
        # Gathers current time in string format to append to the logger file name, to allow distinction between different runs
        append_time = datetime.datetime.utcnow().strftime('%H-%M-%S')
        # Sets up the formatter and handlers needed for the loggers, in GMT time
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
            # Upon successful connection, the server subscribes to the client done topic
            # After that, the system handler thread is started
            self.main_logger.info(f"Connected to the broker at {broker_address}")
            self.client.subscribe(client_done, qos=0)
            self.main_logger.info(f"Subscribed to {client_done} topic with QoS 0")
            self.handler_thread.start()
        else:
            # In case of error during connection the log will contain the error code for debugging
            self.main_logger.info(f"Error connecting to broker, with code {rc}")

        # Callback for when the client object successfully disconnects from the broker
    
    # Callback for when the client object successfully disconnects from the broker
    def on_disconnect(self, client, userdata, rc):
        self.main_logger.info(f"Disconnected from broker at {broker_address}")
    
    # Callback for when the server receives a message on the main topic, on any of the 10 clients
    # Its a callback per client instead of calculating the client on the received message, to try and minimize overhead during the transmission period
    def on_maintopic_c0(self, client, userdata, msg):
        start = time.monotonic()
        if self.run_client_intime[0] == 0:
            self.run_client_start[0] = datetime.datetime.utcnow()
            self.run_client_expected_finish[0] = self.run_client_start[0] + datetime.timedelta(seconds=self.run_expected_time)
        self.run_client_finish[0] = datetime.datetime.utcnow()
        if self.run_client_finish[0] < self.run_client_expected_finish[0]:
            self.run_client_intime[0] += 1
        else:
            self.run_client_late[0] += 1
        # self.timestamp_logger.info(f"Received message #{(self.run_client_intime[0]+self.run_client_late[0])} from the {msg.topic} topic")
        end = time.monotonic()
        print(f"Time taken on callback: {round((end-start)*1000,3)} ms")
    
    def on_maintopic_c1(self, client, userdata, msg):
        if self.run_client_intime[1] == 0:
            self.run_client_start[1] = datetime.datetime.utcnow()
            self.run_client_expected_finish[1] = self.run_client_start[1] + datetime.timedelta(seconds=self.run_expected_time)
        self.run_client_finish[1] = datetime.datetime.utcnow()
        if self.run_client_finish[1] < self.run_client_expected_finish[1]:
            self.run_client_intime[1] += 1
        else:
            self.run_client_late[1] += 1
        self.timestamp_logger.info(f"Received message #{(self.run_client_intime[1]+self.run_client_late[1])} from the {msg.topic} topic")
    
    def on_maintopic_c2(self, client, userdata, msg):
        if self.run_client_intime[2] == 0:
            self.run_client_start[2] = datetime.datetime.utcnow()
            self.run_client_expected_finish[2] = self.run_client_start[2] + datetime.timedelta(seconds=self.run_expected_time)
        self.run_client_finish[2] = datetime.datetime.utcnow()
        if self.run_client_finish[2] < self.run_client_expected_finish[2]:
            self.run_client_intime[2] += 1
        else:
            self.run_client_late[2] += 1
        self.timestamp_logger.info(f"Received message #{(self.run_client_intime[2]+self.run_client_late[2])} from the {msg.topic} topic")
    
    def on_maintopic_c3(self, client, userdata, msg):
        if self.run_client_intime[3] == 0:
            self.run_client_start[3] = datetime.datetime.utcnow()
            self.run_client_expected_finish[3] = self.run_client_start[3] + datetime.timedelta(seconds=self.run_expected_time)
        self.run_client_finish[3] = datetime.datetime.utcnow()
        if self.run_client_finish[3] < self.run_client_expected_finish[3]:
            self.run_client_intime[3] += 1
        else:
            self.run_client_late[3] += 1
        self.timestamp_logger.info(f"Received message #{(self.run_client_intime[3]+self.run_client_late[3])} from the {msg.topic} topic")
    
    def on_maintopic_c4(self, client, userdata, msg):
        if self.run_client_intime[4] == 0:
            self.run_client_start[4] = datetime.datetime.utcnow()
            self.run_client_expected_finish[4] = self.run_client_start[4] + datetime.timedelta(seconds=self.run_expected_time)
        self.run_client_finish[4] = datetime.datetime.utcnow()
        if self.run_client_finish[4] < self.run_client_expected_finish[4]:
            self.run_client_intime[4] += 1
        else:
            self.run_client_late[4] += 1
        self.timestamp_logger.info(f"Received message #{(self.run_client_intime[4]+self.run_client_late[4])} from the {msg.topic} topic")
    
    def on_maintopic_c5(self, client, userdata, msg):
        if self.run_client_intime[5] == 0:
            self.run_client_start[5] = datetime.datetime.utcnow()
            self.run_client_expected_finish[5] = self.run_client_start[5] + datetime.timedelta(seconds=self.run_expected_time)
        self.run_client_finish[5] = datetime.datetime.utcnow()
        if self.run_client_finish[5] < self.run_client_expected_finish[5]:
            self.run_client_intime[5] += 1
        else:
            self.run_client_late[5] += 1
        self.timestamp_logger.info(f"Received message #{(self.run_client_intime[5]+self.run_client_late[5])} from the {msg.topic} topic")
    
    def on_maintopic_c6(self, client, userdata, msg):
        if self.run_client_intime[6] == 0:
            self.run_client_start[6] = datetime.datetime.utcnow()
            self.run_client_expected_finish[6] = self.run_client_start[6] + datetime.timedelta(seconds=self.run_expected_time)
        self.run_client_finish[6] = datetime.datetime.utcnow()
        if self.run_client_finish[6] < self.run_client_expected_finish[6]:
            self.run_client_intime[6] += 1
        else:
            self.run_client_late[6] += 1
        self.timestamp_logger.info(f"Received message #{(self.run_client_intime[6]+self.run_client_late[6])} from the {msg.topic} topic")
    
    def on_maintopic_c7(self, client, userdata, msg):
        if self.run_client_intime[7] == 0:
            self.run_client_start[7] = datetime.datetime.utcnow()
            self.run_client_expected_finish[7] = self.run_client_start[7] + datetime.timedelta(seconds=self.run_expected_time)
        self.run_client_finish[7] = datetime.datetime.utcnow()
        if self.run_client_finish[7] < self.run_client_expected_finish[7]:
            self.run_client_intime[7] += 1
        else:
            self.run_client_late[7] += 1
        self.timestamp_logger.info(f"Received message #{(self.run_client_intime[7]+self.run_client_late[7])} from the {msg.topic} topic")
    
    def on_maintopic_c8(self, client, userdata, msg):
        if self.run_client_intime[8] == 0:
            self.run_client_start[8] = datetime.datetime.utcnow()
            self.run_client_expected_finish[8] = self.run_client_start[8] + datetime.timedelta(seconds=self.run_expected_time)
        self.run_client_finish[8] = datetime.datetime.utcnow()
        if self.run_client_finish[8] < self.run_client_expected_finish[8]:
            self.run_client_intime[8] += 1
        else:
            self.run_client_late[8] += 1
        self.timestamp_logger.info(f"Received message #{(self.run_client_intime[8]+self.run_client_late[8])} from the {msg.topic} topic")
    
    def on_maintopic_c9(self, client, userdata, msg):
        if self.run_client_intime[9] == 0:
            self.run_client_start[9] = datetime.datetime.utcnow()
            self.run_client_expected_finish[9] = self.run_client_start[9] + datetime.timedelta(seconds=self.run_expected_time)
        self.run_client_finish[9] = datetime.datetime.utcnow()
        if self.run_client_finish[9] < self.run_client_expected_finish[9]:
            self.run_client_intime[9] += 1
        else:
            self.run_client_late[9] += 1
        self.timestamp_logger.info(f"Received message #{(self.run_client_intime[9]+self.run_client_late[9])} from the {msg.topic} topic")
    
    # Callback for when the server receives a message on the client done topic
    # If the topic is the client done topic, it means that one of the clients has finished publishing of his messages
    def on_clientdone(self, client, userdata, msg):
        self.run_client_done += 1
        # If the total of clients done equals the amount of clients of the run, run is considered finished
        if self.run_client_done == self.run_client_amount:
            self.client.unsubscribe(main_topic)
            self.run_finished = True
    
    # Function to calculate and output every relevant metric and result to the logger once a run is complete
    def result_logging(self):
        run_msg_counter = sum(self.run_client_intime)+sum(self.run_client_late)
        run_packet_loss = 100-((run_msg_counter/self.run_total_msg_amount)*100)
        run_exec_time = max(self.run_client_finish)-min(self.run_client_start)
        run_actual_freq = 1/(run_exec_time.total_seconds()/(self.run_msg_amount-1))
        run_time_factor = (run_exec_time.total_seconds()/self.run_expected_time)
        run_frequency_factor = (run_actual_freq/self.run_msg_freq)*100
        self.main_logger.info(f"All {self.run_client_amount} clients finished publishing for this execution")
        self.main_logger.debug(f"==================================================")
        self.main_logger.debug(f"RUN RESULTS")
        self.main_logger.info(f"Received {run_msg_counter} out of {self.run_total_msg_amount} messages")
        self.main_logger.info(f"Messages received inside time window: {sum(self.run_client_intime)}")
        self.main_logger.info(f"Messages received outside time window: {sum(self.run_client_late)}")
        self.main_logger.info(f"Calculated packet loss: {round(run_packet_loss,2)}%")
        self.main_logger.info(f"Run start time (of first received message): {min(self.run_client_start).strftime('%H:%M:%S.%f')[:-3]}")
        self.main_logger.info(f"Run expected finish time: {min(self.run_client_expected_finish).strftime('%H:%M:%S.%f')[:-3]}")
        self.main_logger.info(f"Run actual finish time: {max(self.run_client_finish).strftime('%H:%M:%S.%f')[:-3]}")
        self.main_logger.info(f"Expected execution time (for {self.run_msg_amount-1} messages): {round(self.run_expected_time,3)} seconds")
        self.main_logger.info(f"Total execution time (for {self.run_msg_amount-1} messages): {round(run_exec_time.total_seconds(),3)} seconds")
        self.main_logger.info(f"Time factor: {round(run_time_factor,3)}x of the expected time")
        self.main_logger.info(f"Actual frequency: {round(run_actual_freq,2)} Hz")
        self.main_logger.info(f"Frequency factor: {round(run_frequency_factor,2)}%")

    # System handler function, used to feed all clients with each run information and start order. This function is run on a separate thread
    def sys_handler(self):
        # Before the server starts ordering the runs, it will double check the config file to make sure all lists (if existing) have the correct size
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
            for run in range(system_runs):
                # Prints to the console which run it is, for the user to keep track without having to look at the logs
                self.main_logger.debug(f"==================================================")
                self.main_logger.debug(f"PERFORMING RUN {run+1}/{system_runs}...")
                self.timestamp_logger.debug(f"==================================================")
                self.timestamp_logger.debug(f"PERFORMING RUN {run+1}/{system_runs}...")
                # Gathers all the information for the next run to be performed, such as client amount, QoS to be used, message amount, size and publishing frequency
                # This information can be both a list (if it varies over the runs) or an int (if it's the same across all runs)
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
                self.run_total_msg_amount = self.run_msg_amount * self.run_client_amount
                self.run_expected_time = (self.run_msg_amount-1) / self.run_msg_freq
                # Resets all needed variables
                self.run_client_intime = [0] * self.run_client_amount
                self.run_client_late = [0] * self.run_client_amount
                self.run_client_done = 0
                self.run_client_start = [0] * self.run_client_amount
                self.run_client_finish = [0] * self.run_client_amount
                self.run_client_expected_finish = [0] * self.run_client_amount
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
                client_config = json.dumps({"client_amount": self.run_client_amount, "msg_qos": self.run_msg_qos, "msg_amount": self.run_msg_amount, "msg_size": self.run_msg_size, "msg_freq": self.run_msg_freq})
                self.client.publish(begin_client, client_config, qos=0)
                self.main_logger.info(f"Sent configuration and start order to all the clients")
                self.run_finished = False
                # This is used to wait for the previous run to finish and clean things up before starting a new one
                while self.run_finished == False:
                    pass
                self.result_logging()
            # Once all runs are finished, cleans up everything and exits thread
            self.cleanup()
    
    # Cleanup function, to inform all clients all runs are finished and gracefully closes the connection with the broker
    def cleanup(self):
        self.main_logger.debug(f"==================================================")
        self.main_logger.info(f"Performing cleanup of MQTT connection, exiting and informing clients")
        self.client.publish(finish_client, None, qos=0)
        self.client.unsubscribe(main_topic)
        self.client.unsubscribe(client_done)
        for client in range(10):
            self.client.message_callback_remove(f"{main_topic}/client-{client}")
        self.client.message_callback_remove(client_done)
        self.client.disconnect()

    # Starts the class with all the variables necessary
    def __init__(self):
        # Creates the logs folder in case it doesn't exist
        os.makedirs(log_folder, exist_ok=True)
        self.logger_setup()
        self.main_logger.debug(f"==================================================")
        self.main_logger.debug(f"NEW SYSTEM EXECUTION")
        self.run_finished = True
        # Declares the thread where the system handler will run
        self.handler_thread = threading.Thread(target = self.sys_handler, args=())
        self.main_logger.info(f"Creating MQTT Client with ID {client_id}")
        # Starts the MQTT client with specified ID, passed through the input arguments, and defines all callbacks
        self.client = mqtt.Client(client_id=client_id)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        for client in range(10):
            self.client.message_callback_add(main_topic.replace("#", f"client-{client}"), self.on_maintopic_c0)
        self.client.message_callback_add(client_done, self.on_clientdone)
        # The MQTT client connects to the broker and the network loop iterates forever until the cleanup function
        self.client.connect(broker_address, 1883, 3600)
        self.client.loop_forever()

# Starts one MQTT Server class object
mqtt_server = MQTT_Server()
