# Import of all necessary packages and libraries
import paho.mqtt.client as mqtt
import time
import json
import logging
import sys
import threading

# Read the configuration file, which includes topics and system run details
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
system_runs = config['system_details']['different_runs']

# Configures the logger, which will contain all details for further analysis
logging.basicConfig(filename = log_name, filemode = 'a', format = '%(asctime)s %(levelname)s: %(message)s', level = logging.INFO)

# Class of the server code
class MQTT_Server:
    # Callback for when the client object successfully connects to the broker
    def on_connect(self, client, userdata, flags, rc):
        if rc==0:
            # Upon successful connection, the system handler thread is started
            logging.info(f"Connected to the broker at {broker_address}")
            self.handler_thread.start()
        else:
            # In case of error during connection the log will contain the error code for debugging
            logging.info(f"Error connecting to broker, with code {rc}")

    # Callback for when the client object receives a message, which includes topic and payload
    def on_message(self, client, userdata, msg):
        # If the topic is the main one, it is just a normal message during that run, and the run counter will increase
        if str(msg.topic) == main_topic:
            self.run_counter += 1
        # If the topic is the client done topic, it means that one of the clients has finished publishing of his messages
        elif str(msg.topic) == client_done:
            self.clients_done += 1
            # If the total of clients done equals the amount of clients of the run, run is considered finished and packet loss is calculated the logged
            if self.clients_done == self.run_client_amount:
                self.run_loss = 100-((self.run_counter/self.run_msg_amount)*100)
                logging.info(f"All {self.run_client_amount} clients finished publishing for this execution")
                logging.info(f"Received {self.run_counter} out of {self.run_msg_amount} messages, totalling packet loss at {round(self.run_loss,3)}%")
                self.client.unsubscribe(main_topic)
                self.run_finished = True

    # Callback for when the client object successfully disconnects from the broker
    def on_disconnect(self, client, userdata, rc):
        logging.info(f"Disconnected from broker at {broker_address}")

    # System handler function, used to feed all clients with each run information and start order. This function is run on a separate thread
    def sys_handler(self):
        # The config file has a parameter with the amount of system runs to be performed, which will be iterated in here
        for run in range(system_runs):
            # This is used to wait for the previous run to finish and clean things up before starting a new one
            while self.run_finished == False:
                time.sleep(1)

            # Gathers all the information for the next run to be performed, such as client amount, QoS to be used, message amount, size and publishing frequency
            self.run_client_amount = config['system_details']['client_amount'][run]
            self.run_msg_qos = config['system_details']['msg_qos'][run]
            self.run_msg_amount = config['system_details']['msg_amount'][run]
            self.run_msg_size = config['system_details']['msg_size'][run]
            self.run_msg_freq = config['system_details']['msg_freq'][run]
            self.run_total_msg_amount = msg_amount * client_amount
            
            # Subscribes to the message topic with the correct QoS to be used in the run, and logs all the information of the run
            self.client.subscribe(main_topic, qos=msg_qos)
            logging.warning(f"NEW EXECUTION")
            logging.info(f"Subscribed to {main_topic} topic with QoS level {msg_qos}")
            logging.info(f"System details: {client_amount} clients with {msg_amount} messages each using QoS level {msg_qos}, for a total of {total_msg_amount} messages")
            
            # Dumps the information to a JSON payload to send to all the clients, and publishes it to the client topic
            client_config = json.dumps({"msg_qos": msg_qos, "msg_amount": msg_amount, "msg_size": msg_size, "msg_freq": msg_freq})
            self.client.publish(begin_client, client_config, qos=0)
            logging.info(f"Sent configuration and start order to all the clients for run #{run+1}")
            self.run_finished = False
        # Once all runs are finished, cleans up everything and exits thread
        self.cleanup()
        return
    
    # Cleanup function, to inform all clients all runs are finished and disconnects from the broker
    def cleanup(self):
        self.client.publish(begin_client, None, qos=0)
        time.sleep(2)
        self.client.disconnect()

    # Starts the class with all the variables
    def __init__(self):
        self.run_finished = True
        self.clients_done = 0
        self.run_counter = 0
        self.run_loss = 0
        # Declares the thread where the system handler will run
        self.handler_thread = threading.Thread(target = self.sys_handler, args=())
        logging.info(f"Creating MQTT Client with ID {client_id}")
        # Starts the MQTT client with specified ID, and defines all callbacks
        self.client = mqtt.Client(client_id=client_id)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        # The MQTT client connects to the broker and the network loop iterates forever until the cleanup function
        self.client.connect(broker_address, 1883, 60)
        self.client.loop_forever()

# Starts one MQTT Server class object
mqtt_server = MQTT_Server()
