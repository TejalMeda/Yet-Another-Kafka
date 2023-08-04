import selectors
import socket
import json
import time

dicConsumers = {}
content = {}
protocolCon = {}


sel = selectors.DefaultSelector()

def accept(sock, mask):
    conn, addr = sock.accept() # Should be ready
    
    print('accepted', conn, 'from', addr)
    data = conn.recv(1000)

    #receives the record message from the producer/consumer
    protocol, Midtype, topic = AckDecode(data)
    topics = []
    if(topic.count('/') > 1):
        topics = topic[1:].split('/')
    else:
        topics.append(topic[1:])
        fp = open('topics/'+topic+'.txt', 'a')
        fp.close()
    
    #associates the corresponding protocol to its conn
    protocolCon[conn] = protocol

    #se for consumer
    if(Midtype == 'MiddlewareType.CONSUMER'):
        dicConsumers[conn] = topics[len(topics)-1] #associates yours with the subscribed topic
        topico = topics[len(topics)-1]
        if(topico in content and content[topico] != None):
            message = content[topico]
            if(protocolCon[conn] == 'ProtocolType.JSON'):
                jsonT = jsonEncode(topico, message, '')
                conn.send(jsonT)
           
        #topic SUBSCRIBE message
        print('SUB FROM ', conn, ' TO TOPIC :', topic)

    sel.register(conn, selectors.EVENT_READ, read)



def read(conn, mask):
    data = conn.recv(1000)

    if data:
        #according to the producer's protocol, decode the message
        if(protocolCon[conn] == 'ProtocolType.JSON'):
            method, topic, message = jsonDecode(data)
        

        topics = []
        if(topic.count('/') > 1):
            topics = topic[1:].split('/')
        else:
            topics.append(topic[1:])
        
        #in the case of publication, it is necessary to send the message to the corresponding consumers
        if method == 'PUB':
            #save the message in the corresponding topic
            for t in topics:
                content[t] = message
                f = open('topics/'+t+'.txt', 'a')
                f.write(str(message)+"\n")
                f.close()
            #PUBLISH message in a thread
            print('PUB IN TOPIC: ', topic, ' THE MESSAGE: ', message)
            #travels through consumers
            for c in dicConsumers:
                #scroll through parent and child topics
                for t in topics:
                    #in case there is a topic match
                    if dicConsumers[c] == t:
                        #according to the consumer's protocol, it encodes the message and sends it to the consumer
                        if(protocolCon[c] == 'ProtocolType.JSON'):
                            jsonT = jsonEncode(topic, message, method)
                            c.send(jsonT)
                       
        
        #in case the method is for listing topics
        if method == 'LIST':
            method = "Listagem de tópicos: \n"
            #in case there are topics
            if(len(content) > 0):
                message = []
                #add existing topics
                for topic in content:
                    message.append(topic)
                #according to the consumer protocol, it encodes and sends the list of topics
                if(protocolCon[conn] == 'ProtocolType.JSON'):
                    jsonT = jsonEncode(topic, message, method)
                    conn.send(jsonT)
                  
            #in case there are no topics
            else:
                message = "no topics!"
                if(protocolCon[conn] == 'ProtocolType.JSON'):
                    jsonT = jsonEncode(topic, message, method)
                    conn.send(jsonT)
               
        #in case the method is for the cancellation of the subscription
        if method == 'CANCEL':
            if(conn in dicConsumers):
                del dicConsumers[conn]
            del protocolCon[conn]
            print('CANCELED SUB FROM TOPIC: ', topic, 'BY: ', conn)
    #when the producer or consumer disconnects
    else:
        print('closing', conn)
        if(conn in dicConsumers):
            del dicConsumers[conn]
            print('CANCELED SUB BY: ', conn)
        del protocolCon[conn] 
        sel.unregister(conn)
        conn.close()

#decode in JSON
def jsonDecode(data):
    data = data.decode('utf-8')
    jsonText = json.loads(data)
    method = jsonText['METHOD']
    topic = jsonText['TOPIC']
    message = jsonText['MESSAGE_CONTENT']
    return method, topic, message

#encode in JSON
def jsonEncode(topic, message, method):
    jsonText = {'METHOD' : method, 'TOPIC' : topic, 'MESSAGE_CONTENT': message}
    jsonText = json.dumps(jsonText)
    jsonText = jsonText.encode('utf-8')
    return jsonText

#log message decode
def AckDecode(data):
    data = data.decode('utf-8')
    jsonText = json.loads(data)
    protocol = jsonText['PROTOCOL']
    Midtype = jsonText['TYPE']
    topic = jsonText['TOPIC']
    return protocol, Midtype, topic

sock = socket.socket()
sock.bind(('', 8009))
sock.listen(100)
sel.register(sock, selectors.EVENT_READ, accept)

while True:
    events = sel.select()
    for key, mask in events:
        callback = key.data
        callback(key.fileobj, mask)
