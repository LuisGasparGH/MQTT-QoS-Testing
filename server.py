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

counter = 0
loss = 0

def on_connect(client, userdata, flags, rc):
    if rc==0:
        print(f"Connected: {broker_address}")
    else:
        print(f"Error: RC = {rc}")

    client.subscribe(main_topic, 2)
    client.subscribe(tx_done, 0)
    print(f"Subscribed: {main_topic}, {tx_done}")
    print("Sending: begin client")
    client.publish(begin_client, None, 0)

def on_message(client, userdata, msg):
    global counter
    if str(msg.topic) == main_topic:
        msg_handler(client)
    # elif str(msg.topic) == tx_done:
    #     finish_handler(client, counter)

def on_disconnect(client, userdata, rc):
    print("Disconnect: broker")

def msg_handler(client):
    global counter
    counter += 1
    loss = (100-(counter/msg_amount)*100)
    print(f"Sub: M{counter} | Loss %: {loss}%", end="\r")

# def finish_handler(client, counter):
    # print(f"Total messages received: {counter}/{msg_amount}")
    # print("Sending: finish client")
    # client.publish(finish_client, None, 0)
    # client.disconnect()

print(f"Setup: MQTT id {client_id}")
client = mqtt.Client(client_id=client_id)
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect
client.connect(broker_address, 1883, 60)
client.loop_forever()
