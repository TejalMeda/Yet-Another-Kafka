from enum import Enum
import socket
import sys
import fcntl
import os
import selectors
import json
import time
import time

orig_fl = fcntl.fcntl(sys.stdin, fcntl.F_GETFL)
fcntl.fcntl(sys.stdin, fcntl.F_SETFL, orig_fl | os.O_NONBLOCK)

class MiddlewareType(Enum):
    CONSUMER = 1
    PRODUCER = 2

class ProtocolType(Enum):
    JSON = 1

class Queue:
    def __init__(self, topic, protocol, Mtype):
        self.topic = topic
        self.HOST = 'localhost' 
        self.PORT = 8009
        self.selector = selectors.DefaultSelector()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.HOST, self.PORT))
        self.selector.register(self.socket, selectors.EVENT_READ, self.pull)
        self.protocol = protocol
        self.type = Mtype
        #initial identity registration message on the broker
        self.AckMessage(self.protocol, self.type, self.topic)

    def push(self, value): 
        #publish message to broker  
        self.message('PUB', value) 
        print(self.topic, value)
        pass

    def pull(self):
        #consumer blocked until receiving date
        data = self.socket.recv(1000)
        if data:
            method, topic, message = self.decode(data)
            return topic, message
        else:
            pass

    # function to send messages to the broker
    def message(self, method, data):
        #encode and send the message
        data = self.encode(self.topic, data, method)
        self.socket.send(data)
    
    #function for the initial acknowledgment of the producer/consumer that informs the broker of its protocol, whether it is a consumer or a producer and what the topic is (done in JSON)
    def AckMessage(self, protocol, Midtype, topic):
        prot = str(protocol)
        Mtype = str(Midtype)
        jsonText = {'PROTOCOL' : prot, 'TYPE' : Mtype, 'TOPIC' : topic}
        jsonText = json.dumps(jsonText)
        jsonText = jsonText.encode('utf-8')
        self.socket.send(jsonText)

    #function that returns the last message of the topic that the consumer subscribed to
    def lastMessage(self):
        self.message('LAST MESSAGE', "")
    
    def cancelTopic(self):
        self.message('CANCEL', "")


class JSONQueue(Queue):
    def __init__(self, topic, type=MiddlewareType.CONSUMER, protocol=ProtocolType.JSON):
        super().__init__(topic, protocol, type)

    #encode in JSON
    def encode(self, topic, message, method):
        jsonText = {'METHOD' : method, 'TOPIC' : topic, 'MESSAGE_CONTENT': message}
        jsonText = json.dumps(jsonText)
        jsonText = jsonText.encode('utf-8')
        return jsonText

    #decode in JSON
    def decode(self, content):
        content = content.decode('utf-8')
        jsonText = json.loads(content)
        method = jsonText['METHOD']
        topic = jsonText['TOPIC']
        message = jsonText['MESSAGE_CONTENT']
        return method, topic, message

