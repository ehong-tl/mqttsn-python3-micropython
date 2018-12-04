"""
/*******************************************************************************
 * Copyright (c) 2011, 2013 IBM Corp.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Eclipse Distribution License v1.0 which accompany this distribution. 
 *
 * The Eclipse Public License is available at 
 *    http://www.eclipse.org/legal/epl-v10.html
 * and the Eclipse Distribution License is available at 
 *   http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * Contributors:
 *    Ian Craggs - initial API and implementation and/or initial documentation
 *    EH Ong - port to Python 3 and Micropython
 *******************************************************************************/
"""

import MQTTSN
import time, sys, socket

debug = False

class Receivers:

  def __init__(self, socket):
    #print("initializing receiver")
    self.socket = socket
    self.connected = False
    self.observe = None
    self.observed = []

    self.inMsgs = {}
    self.outMsgs = {}

    self.puback = MQTTSN.Pubacks()
    self.pubrec = MQTTSN.Pubrecs()
    self.pubrel = MQTTSN.Pubrels()
    self.pubcomp = MQTTSN.Pubcomps()

  def lookfor(self, msgType):
    self.observe = msgType

  def waitfor(self, msgType, msgId=None):
    msg = None
    count = 0
    while True:
      while len(self.observed) > 0:
        msg = self.observed.pop(0)
        if msg.mh.MsgType == msgType and (msgId == None or msg.MsgId == msgId):
          break
        else:
          msg = None
      if msg != None:
        break
      time.sleep(0.2)
      count += 1
      if count == 25:
        msg = None
        break
    self.observe = None
    return msg

  def receive(self, topicmap, callback=None):
    packet = None
    try:
      packet, address = MQTTSN.unpackPacket(MQTTSN.getPacket(self.socket))
    except:
      if sys.exc_info()[0] != socket.timeout:
        #print("unexpected exception", sys.exc_info())
        raise sys.exc_info()
    if packet == None:
      time.sleep(0.1)
      return
    elif debug:
      print(packet)

    if self.observe == packet.mh.MsgType:
      #print("observed", packet)
      self.observed.append(packet)
        
    elif packet.mh.MsgType == MQTTSN.ADVERTISE:
      if hasattr(callback, "advertise"):
        callback.advertise(address, packet.GwId, packet.Duration)

    elif packet.mh.MsgType == MQTTSN.REGISTER:
      topicmap.register(packet.TopicId, packet.TopicName)

    elif packet.mh.MsgType == MQTTSN.PUBACK:
      "check if we are expecting a puback"
      if packet.MsgId in self.outMsgs and \
        self.outMsgs[packet.MsgId].Flags.QoS == 1:
        del self.outMsgs[packet.MsgId]
        if hasattr(callback, "published"):
          callback.published(packet.MsgId)
      else:
        raise Exception("No QoS 1 message with message id "+str(packet.MsgId)+" sent")

    elif packet.mh.MsgType == MQTTSN.PUBREC:
      if packet.MsgId in self.outMsgs:
        self.pubrel.MsgId = packet.MsgId
        self.socket.send(self.pubrel.pack())
      else:
        raise Exception("PUBREC received for unknown msg id "+ \
                    str(packet.MsgId))

    elif packet.mh.MsgType == MQTTSN.PUBREL:
      "release QOS 2 publication to client, & send PUBCOMP"
      msgid = packet.MsgId
      if packet.MsgId not in self.inMsgs:
        pass # what should we do here?
      else:
        pub = self.inMsgs[packet.MsgId]
        topicname = topicmap.registered[pub.TopicId]
        if callback == None or \
           callback.messageArrived(topicname, pub.Data, 2, pub.Flags.Retain, pub.MsgId):
          del self.inMsgs[packet.MsgId]
          self.pubcomp.MsgId = packet.MsgId
          self.socket.send(self.pubcomp.pack())
        if callback == None:
          return (topicname, pub.Data, 2, pub.Flags.Retain, pub.MsgId)

    elif packet.mh.MsgType == MQTTSN.PUBCOMP:
      "finished with this message id"
      if packet.MsgId in self.outMsgs:
        del self.outMsgs[packet.MsgId]
        if hasattr(callback, "published"):
          callback.published(packet.MsgId)
      else:
        raise Exception("PUBCOMP received for unknown msg id "+ \
                    str(packet.MsgId))

    elif packet.mh.MsgType == MQTTSN.PUBLISH:
      "finished with this message id"
      if packet.Flags.QoS in [0, 3]:
        qos = packet.Flags.QoS
        topicname = topicmap.registered[packet.TopicId]
        data = packet.Data
        if qos == 3:
          qos = -1
          if packet.Flags.TopicIdType == MQTTSN.TOPICID:
            topicname = packet.Data[:packet.TopicId]
            data = packet.Data[packet.TopicId:]
        if callback == None:
          return (topicname, data, qos, packet.Flags.Retain, packet.MsgId)
        else:
          callback.messageArrived(topicname, data, qos, packet.Flags.Retain, packet.MsgId)
      elif packet.Flags.QoS == 1:
        topicname = topicmap.registered[packet.TopicId]
        if callback == None:
          return (topicname, packet.Data, 1,
                           packet.Flags.Retain, packet.MsgId)
        else:
          if callback.messageArrived(topicname, packet.Data, 1,
                           packet.Flags.Retain, packet.MsgId):
            self.puback.MsgId = packet.MsgId
            self.socket.send(self.puback.pack())
      elif packet.Flags.QoS == 2:
        self.inMsgs[packet.MsgId] = packet
        self.pubrec.MsgId = packet.MsgId
        self.socket.send(self.pubrec.pack())

    else:
      raise Exception("Unexpected packet"+str(packet))
    return packet

  def __call__(self, callback, topicmap, queue):
    try:
      while True:
        self.receive(topicmap, callback)
    except:
      queue.put(sys.exc_info())
      if sys.exc_info()[0] != socket.error:
        #print("unexpected exception", sys.exc_info())
        pass
