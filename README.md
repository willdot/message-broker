# Message Broker

Message broker is my attempt at building client / server pub/sub system written in Go using plain `net.Conn`. 

I decided to try to build one to further my understanding of how such a common tool is used in software engineering. 

I realised that I took for granted how complicated other pub / sub message brokers were such as NATs, RabbitMQ and Kafka. By creating my own, I hope to dive into and understand more of how message brokers work. 

## Concept

The server accepts TCP connections. When a connection is first established, the client will send an "action" message which determines what type of client it is; subscribe or publish. 

### Subscribing
Once a connection has declared itself as a subscribing connection, it will need to then send the list of topics it wishes to initally subscribe to. After that the connection will enter a loop where it can then send a new action; subscribe to new topic(s) or unsubscribe from topic(s).

### Publishing
Once a subscription has declared itself as a publisher, it will enter a loop where it can then send a message for a topic. Once a message has been received, the server will then send it to all connections that are subscribed to that topic.

### Sending data via a connection

When sending a message representing an action (subscribe, publish etc) then a uint16 binary message is sent. 

When sending any other data, the length of the data is to be sent first using a binary uint32 and then the actual data sent afterwards. 