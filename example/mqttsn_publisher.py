from mqttsn.MQTTSNclient import Client
import struct
import time
import sys

class Callback:

    def published(self, MsgId):
        print("Published")

def connect_gateway():
    try:
        while True:
            try:
                aclient.connect()
                print('Connected to gateway...')
                break
            except:
                print('Failed to connect to gateway, reconnecting...')
                time.sleep(1)
    except KeyboardInterrupt:
        print('Exiting...')
        sys.exit()

def register_topic():
    global topic1, topic2, topic3
    topic1 = aclient.register("topic1")
    print("topic1 registered.")
    topic2 = aclient.register("topic2")
    print("topic2 registered.")
    topic3 = aclient.register("topic3")
    print("topic3 registered.")

aclient = Client("client_sn_pub", "10.42.0.1", port=10000)
aclient.registerCallback(Callback())
connect_gateway()

topic1 = None
topic2 = None
topic3 = None
register_topic()

payload1 = 'Hello World!'
payload2 = struct.pack('BBB', 3,2,1)
payload3 = struct.pack('d', 3.14159265359)

pub_msgid = aclient.publish(topic1, payload1, qos=0)
time.sleep(1)
pub_msgid = aclient.publish(topic2, payload2, qos=1)
time.sleep(1)
pub_msgid = aclient.publish(topic3, payload3, qos=2)
time.sleep(1)

aclient.disconnect()
print("Disconnected from gateway.")
