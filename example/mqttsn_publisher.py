from mqttsn.MQTTSNclient import Client
import struct
import time
import sys

class Callback:

    def __init__(self):
        self.registered = {}

    def published(self, MsgId):
        if pub_msgid == MsgId:
            print("Published")

    def register(self, TopicId, TopicName):
        self.registered[TopicId] = TopicName

def connect_broker():
    try:
        while True:
            try:
                aclient.connect()
                print('Connected to broker...')
                break
            except:
                print('Failed to connect to broker, reconnecting...')
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

aclient = Client("client_sn_pub", "10.42.0.1", port=1884)
aclient.registerCallback(Callback())
connect_broker()

topic1 = None
topic2 = None
register_topic()

payload1 = struct.pack('BBBB', 1,2,3,4)
i = 0
try:
    while True:
        if not aclient.queue.empty():
            exp = aclient.queue.get()
            err = str(exp[1]).split(', ')[1]
            if err == 'DISCONNECT':
                connect_broker()
                register_topic()
        else:
            i += 1
            pub_msgid = aclient.publish(topic1, payload1, qos=2)
            time.sleep(1)
            pub_msgid = aclient.publish(topic2, str(i), qos=2)
            time.sleep(1)
except KeyboardInterrupt:
    aclient.disconnect()
    print("Disconnectd from broker.")
