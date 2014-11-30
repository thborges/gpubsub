package main

import (
	"ppd/gpubsub"
	"fmt"
	"time"
	"os"
)

var messages int;

func process_messages(message []byte, err error) {
	if err != nil {
		//fmt.Printf("Erro ao decodificar: %s\n", err);
	} else {
		//fmt.Printf("Received message from publisher\n");
		messages++;
	}
}

func main() {

	if len(os.Args) < 1 {
		fmt.Printf("Informe o topico.\n");
		return;
	}
		
	messages = 0;
	p := gpubsub.Subscriber{}
	err := p.Connect("localhost:8999")
	if err != nil {
		fmt.Printf("Erro ao conectar: %s\n", err);
		return
	}
	
	err = p.Subscribe(os.Args[1], process_messages)
	if err != nil {
		fmt.Printf("Erro ao se increver: %s\n", err);
		return
	}
	
	for {
		mymess := messages;
		time.Sleep(time.Second);
		fmt.Printf("%d messages per second. %d messages received.\n", messages-mymess, messages);
	}	
}