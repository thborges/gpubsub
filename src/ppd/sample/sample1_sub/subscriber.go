package main

import (
	"ppd/gpubsub"
	"fmt"
	"time"
	"os"
	"ppd/sample/common"
	"encoding/gob"
)

var messages int;

func EventReceived(event interface{}) {
	messages++;
	//quote := event.(*common.Quote)
	//fmt.Printf("Quote received %f\n", quote.GetQuote());
}

func main() {
	gob.Register(common.Quote{})

	if len(os.Args) < 2 {
		fmt.Printf("Informe o servidor e o topico.\n");
		return;
	}
		
	p := gpubsub.Subscriber{}
	err := p.Connect(os.Args[1])
	if err != nil {
		fmt.Printf("Erro ao conectar: %s\n", err);
		return
	}
	
	messages = 0;
	quote := common.Quote{}
	err = p.Subscribe(os.Args[2], EventReceived, &quote)
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