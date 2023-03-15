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
msg_amount = config['msg_details']['amount']
msg_size = config['msg_details']['size']
msg_freq = config['msg_details']['freq']
client_id = config['client_details']['ids']['client-0']

print("Client message configuration:")
print(f"\tAmount: {msg_amount} messages")
print(f"\tSize: {msg_size} bytes")
print(f"\tFrequency: {msg_freq}Hz")

def on_connect(client, userdata, flags, rc):
    if rc==0:
        print(f"Connected: {broker_address}")
    else:
        print(f"Error: RC = {rc}")

    client.subscribe(begin_client, 0)
    client.subscribe(finish_client, 0)
    print(f"Subscribed: {begin_client}, {finish_client}")
    print("Waiting: begin client")

def on_message(client, userdata, msg):
    if str(msg.topic) == begin_client:
        msg_handler(client)
    elif str(msg.topic) == finish_client:
        finish_handler(client)

def on_publish(client, userdata, mid):
    print(f"Handshake: M{mid-2}", end="\r")

def on_disconnect(client, userdata, rc):
    print("Disconnect: broker")

def msg_handler(client):
    print("Sleep: 2 seconds")
    time.sleep(2)
    payload = bytearray(msg_size)
    for msg in range(msg_amount):
        print(f"Pub: M{msg+1}", end="\r")
        client.publish(main_topic, payload, qos=2)
        time.sleep(1/msg_freq)
    
    # print("Sending: tx done")
    # client.publish(tx_done, None, 0)

# def finish_handler(client):
#     print("Received: finish client")
#     client.disconnect()

print(f"Setup: MQTT id {client_id}")
client = mqtt.Client(client_id=client_id)
client.on_connect = on_connect
client.on_message = on_message
client.on_publish = on_publish
client.on_disconnect = on_disconnect
client.connect(broker_address, 1883, 60)
client.loop_forever()