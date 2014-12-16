package gpubsub

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"net"
)

type Broker struct {
	topics        map[string](map[net.Addr]Listener)
	messageBuffer map[string]chan Message
	bufferSize    int
}

type Listener struct {
	conn    net.Conn
	channel chan []byte
	gone	chan bool
}

func (b *Broker) Start(url string, bufferSize int, simultConns int) error {
	ln, err := net.Listen("tcp", url)
	if err != nil {
		return err
	}

	b.topics = make(map[string](map[net.Addr]Listener))
	b.messageBuffer = make(map[string]chan Message)
	b.bufferSize = bufferSize

	openconns := make(chan int, simultConns)
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
	//TODO: protect to prevent race cond.
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
	//TODO: protect to prevent race cond.
	_, ok := b.topics[topic]
	if !ok {
		b.topics[topic] = make(map[net.Addr]Listener)
		b.messageBuffer[topic] = make(chan Message, b.bufferSize)
		go b.dispatcher(topic)
	}
}

func (b *Broker) writeToSub(topic string, lst Listener) {
	for {
		bytes := <- lst.channel
		_, err := lst.conn.Write(bytes)
		//enc := gob.NewEncoder(conn)
		//err := enc.Encode(m)
		if err != nil {
			lst.conn.Close()
			fmt.Printf("Subscriber %s will be removed because of: %s.\n", lst.conn.RemoteAddr(), err)
			lst.gone <- true
			break
		}
	}
}

func (b *Broker) addSubscriber(topic string, c net.Conn) {
	//TODO: protect to prevent race cond.
	subscribers := b.topics[topic]
	lst := Listener{} 
	lst.conn = c
	lst.channel = make(chan []byte, 10)
	lst.gone = make(chan bool)
	subscribers[c.RemoteAddr()] = lst
	go b.writeToSub(topic, lst)
}

func (b *Broker) dispatcher(topic string) {

	subs := b.topics[topic]
	messages := b.messageBuffer[topic]
	for {
		m := <-messages

		if len(subs) == 0 {
			continue
		}

		var mbytes bytes.Buffer
		enc := gob.NewEncoder(&mbytes)
		enc.Encode(m)

		bytes := mbytes.Bytes()
		for _, lst := range subs {
			select {
				case <- lst.gone: {
					fmt.Printf("Subscriber %s removed.\n", lst.conn.RemoteAddr());
					delete(subs, lst.conn.RemoteAddr())
					close(lst.channel)
				}
				case lst.channel <- bytes[0:]:
			}
		}
	}
}

func (b *Broker) publish(dec *gob.Decoder, conn net.Conn) {

	for {
		m := Message{}
		err := dec.Decode(&m)
		if err == io.EOF {
			conn.Close()
			break
		}

		if err == nil {
			mb, ok := b.messageBuffer[m.Topic]
			if ok {
				mb <- m
			}
		} else {
			fmt.Printf("Error on publish: %s\n", err)
		}

	}
}

func (b *Broker) handleConnection(c net.Conn, openconns chan int) {
	dec := gob.NewDecoder(c)
	m := Message{}
	dec.Decode(&m)
	fmt.Printf("Connection received from %s - %+v\n", c.RemoteAddr(), m)

	switch m.Type {
	case Sub:
		b.checkTopicExists(m.Topic)
		b.addSubscriber(m.Topic, c)
		break
	case Pub:
		b.publish(dec, c)
		break
	}
	<-openconns
}
