from mqttsn.MQTTSNclient import Client
import struct
import time
import sys

class Callback:

    def __init__(self):
        self.registered = {}

    def published(self, MsgId):
        print("Published")

    def register(self, TopicId, TopicName):
        self.registered[TopicId] = TopicName

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
    global topic1, topic2
    topic1 = aclient.register("topic1")
    print("topic1 registered.")
    topic2 = aclient.register("topic2")
    print("topic2 registered.")

aclient = Client("client_sn_pub", "10.42.0.1", port=10000)
aclient.registerCallback(Callback())
connect_gateway()

topic1 = None
topic2 = None
register_topic()

payload1 = struct.pack('BBBB', 1,2,3,4)
payload2 = 'Hello World!'

pub_msgid = aclient.publish(topic1, payload1, qos=2)
time.sleep(1)
pub_msgid = aclient.publish(topic2, payload2, qos=2)
time.sleep(1)

aclient.disconnect()
print("Disconnected from gateway.")
