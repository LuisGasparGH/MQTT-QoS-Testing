import paho.mqtt.client as mqtt
import time
import json

with open("config.json", "r") as config_file:
    config = json.load(config_file)
    print("Config: read")

broker_address = config['broker_address']
main_topic = config['topics']['main_topic']
tx_done = config['topics']['tx_done']
begin_client = config['topics']['begin_client']
finish_client = config['topics']['finish_client']
client_msg_amount = config['msg_details']['amount']
client_amount = config['client_details']['amount']
client_id = config['client_details']['ids']['server']
msg_amount = client_msg_amount*client_amount

print(f"System configuration:")
print(f"\tNumber of clients: {client_amount}")
print(f"\tMessages per client: {client_msg_amount}")
print(f"\tTotal message amount expected: {msg_amount}")

class MQTT_Server:
    def on_connect(self, client, userdata, flags, rc):
        if rc==0:
            print(f"Connected: {broker_address}")
        else:
            print(f"Error: RC = {rc}")

        self.client.subscribe(main_topic, 2)
        self.client.subscribe(tx_done, 0)
        print(f"Subscribed: {main_topic}, {tx_done}")
        print("Sending: begin client")
        self.client.publish(begin_client, None, 0)

    def on_message(self, client, userdata, msg):
        if str(msg.topic) == main_topic:
            self.msg_handler()
        # elif str(msg.topic) == tx_done:
        #     self.finish_handler()

    def on_disconnect(self, client, userdata, rc):
        print("Disconnect: broker")

    def msg_handler(self):
        self.counter += 1
        self.loss = (100-(self.counter/msg_amount)*100)
        print(f"Sub: M{self.counter} | Loss %: {self.loss}%", end="\r")

    # def finish_handler(self):
        # print(f"Total messages received: {self.counter}/{msg_amount}")
        # print("Sending: finish client")
        # self.client.publish(finish_client, None, 0)
        # self.client.disconnect()

    def __init__(self):
        self.counter = 0
        self.loss = 0
        print(f"Setup: MQTT id {client_id}")
        self.client = mqtt.Client(client_id=client_id)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        self.client.connect(broker_address, 1883, 60)
        self.client.loop_forever()

mqtt_server = MQTT_Server()