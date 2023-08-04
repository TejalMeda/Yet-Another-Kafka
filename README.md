# BD2_369_376_377_379
Project Title: Yet another Kafka (YaK)

Team Members: 

Tejal Meda-PES2UG20CS369

Vaishali Ganeshkumar- PES2UG20CS376

Vaishnavi N- PES2UG20CS377

Vaisnavi-PES2UG20CS379

Our project is based on the kafka model which consists of a zookeeper, brokers, producers and consumers.
Through the middleware, this design facilitates communication between N producers and N consumers using a variety of topics and subtopics.
Topics used in this implementation are :

•	temp

•	msg

•	weather

Broker uses the JSON dictionary. After that, the "dicConsumers" dictionary stores pairs of "conn, topic," meaning that for each identity it saves the more specific topic to which you have subscribed. 

Software Architecture

When a producer connects, a message identifying his topic and intended publication location is sent to the broker. The broker receives the same message each time a consumer connects. Additionally, the most recent message of the subscribed topic is provided back to the consumers, if any. The reason it is done in this manner is so the broker can quickly learn the details of the entities and save them. These initial messages are exchanged between the middleware and the broker are all done in JSON.

The information produced by the middleware of the "pull" function is given to the consumer, which is waiting in a loop.
Consumers cannot utilise the functionalities for listing and cancelling topics because they are only implemented in middleware and the broker.
We tested the solution in various ways because it is designed for N consumers, N producers, and hierarchical themes.

