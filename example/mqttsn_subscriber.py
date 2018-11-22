from mqttsn.MQTTSNclient import Client
import struct
import time
import sys

class Callback:

    def __init__(self):
        self.registered = {}

    def messageArrived(self, topicName, payload, qos, retained, msgid):
        print('Got msg %s from %s' % (payload, topicName))
        return True

    def register(self, topicId, topicName):
        self.registered[topicId] = topicName

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

def subsribe_topic():
    aclient.subscribe("topic2", qos=2)
    print("Subscribed to topic2.")
    aclient.subscribe("topic1", qos=2)
    print("Subscribed to topic1.")

aclient = Client("client_sn_sub", "10.42.0.1", port=1884)
aclient.registerCallback(Callback())
connect_broker()

subsribe_topic()

try:
    while True:
        if not aclient.queue.empty():
            exp = aclient.queue.get()
            err = str(exp[1]).split(', ')[1]
            if err == 'DISCONNECT':
                connect_broker()
                subsribe_topic()
        else:
            time.sleep(1)
except KeyboardInterrupt:
    aclient.unsubscribe('topic1')
    print("Unsubscribe from topic1.")
    aclient.unsubscribe('topic2')
    print("Unsubscribe from topic2.")
    aclient.disconnect()
    print("Disconnectd from broker.")
