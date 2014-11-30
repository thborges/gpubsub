package gpubsub

import (
	"net"
	"encoding/gob"
	"bytes"
	"io"
)

type Publisher struct {
	conn net.Conn
	enc *gob.Encoder
	stop bool
}

type Subscriber struct {
	conn net.Conn
	stop bool
}

type SubscriberCallback func(b []byte, err error)

func (p *Subscriber) Connect(url string) error {
	conn, err := net.Dial("tcp", url)
	if err != nil {
		return err
	}
	p.conn = conn
	p.stop = false
	return nil
}

func (p *Subscriber) Subscribe(topic string, sc SubscriberCallback) error {

	var buff bytes.Buffer 
	enc := gob.NewEncoder(&buff)

	m := Message{Sub, topic, nil}
	err := enc.Encode(m)
	p.conn.Write(buff.Bytes())
	
	go func() {
		dec := gob.NewDecoder(p.conn)
		n := Message{}
		for ; !p.stop;  {
			//TODO: Put reader timetout to cancel more cleanly
			err := dec.Decode(&n)
			if err == io.EOF {
				p.conn.Close();
				break;
			}
			sc(n.Data, err)
		}
	}()
	
	return err
}

func (p *Subscriber) Unsubscribe(topic string) {
	p.stop = true
}

func (p *Subscriber) Disconnect() {
	p.conn.Close()
}


/* ----------------------------------- */

func (p *Publisher) Connect(url string) error {
	conn, err := net.Dial("tcp", url)
	if err != nil {
		return err
	}
	p.conn = conn
	p.stop = false
	p.enc = gob.NewEncoder(p.conn)

	m := Message{Pub, "", nil}
	err = p.enc.Encode(m)
	return err
}

func (p *Publisher) Publish(topic string, data []byte) error {
	m := Message{Pub, topic, data}
	err := p.enc.Encode(m)
	return err
}

func (p *Publisher) Disconnect() {
	p.conn.Close()
}