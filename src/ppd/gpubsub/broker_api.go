package gpubsub

import (
	"encoding/gob"
	"fmt"
	"net"
	"io"
)

type Broker struct {
	topics map[string](map[net.Addr]net.Conn)
	messageBuffer chan Message
}

func (b *Broker) Start(url string, bufferSize int) error {
	ln, err := net.Listen("tcp", url)
	if err != nil {
		return err
	}
	
	b.topics = make(map[string](map[net.Addr]net.Conn))
	b.messageBuffer = make(chan Message, bufferSize)
	
	go b.dispatcher()
	
	var openconns chan int;
	openconns = make(chan int, 20);
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Print(err)
			continue
		}
		openconns <- 0
		go b.handleConnection(conn, openconns)
	}
}

func (b *Broker) PrintTopics() {
	fmt.Printf("\n\n[Topics]\n")
	for t, subs := range b.topics {
		fmt.Printf("\t%s\n", t)
		for addr, _ := range subs {
			fmt.Printf("\t\t%s, %s\n", addr.String(), addr.Network())
		} 
	}
	fmt.Printf("\n")
}

func (b *Broker) checkTopicExists(topic string) {
	//fmt.Printf("Topico: %s\n", topic)
	_, ok := b.topics[topic]
	if !ok {
		b.topics[topic] = make(map[net.Addr]net.Conn)
	}
}

func (b *Broker) addSubscriber(topic string, c net.Conn) {
	subscribers := b.topics[topic];
	subscribers[c.RemoteAddr()] = c;
}

func (b *Broker) dispatcher() {
	for {
		m := <-b.messageBuffer

		/*var mbytes bytes.Buffer
		enc := gob.NewEncoder(&mbytes);
		enc.Encode(m)
		
		bytes := mbytes.Bytes()*/
		subs := b.topics[m.Topic]
		for _, conn := range subs {
			go func() {
				//_, err := conn.Write(bytes)
				enc := gob.NewEncoder(conn)
				err := enc.Encode(m)
				if err != nil {
					fmt.Printf("Subscriber %s removed because of: %s.\n", conn.RemoteAddr(), err);
					delete(subs, conn.RemoteAddr())	
				}
			}()
		}
	}
}

func (b *Broker) publish(dec *gob.Decoder, conn net.Conn) {

	for {
		m := Message{}
		err := dec.Decode(&m);
		if err == io.EOF {
			conn.Close();
			break;
		}

		if err == nil {
			b.messageBuffer <- m
		} else {
			fmt.Printf("Error on publish: %s\n", err);
		}
		
	}
}

func (b *Broker) handleConnection(c net.Conn, openconns chan int) {
	dec := gob.NewDecoder(c)
	m := Message{}
	dec.Decode(&m)
	fmt.Printf("Connection received from %s - %+v\n", c.RemoteAddr(), m)
	
	switch (m.Type) {
		case Sub:
			b.checkTopicExists(m.Topic)
			b.addSubscriber(m.Topic, c)
			break
		case Pub:
			b.publish(dec, c)
			break;
	}
	<-openconns
}
