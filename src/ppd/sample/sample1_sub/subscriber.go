package main

import (
	"encoding/gob"
	"fmt"
	"os"
	"ppd/gpubsub"
	"ppd/sample/common"
	"time"
)

var messages int

func EventReceived(event interface{}) {
	messages++
	//quote := event.(*common.Quote)
	//fmt.Printf("Quote received %f\n", quote.GetQuote());
}

func main() {
	gob.Register(common.Quote{})

	if len(os.Args) < 2 {
		fmt.Printf("Informe o servidor e o topico.\n")
		return
	}

	p := gpubsub.Subscriber{}
	err := p.Connect(os.Args[1])
	if err != nil {
		fmt.Printf("Erro ao conectar: %s\n", err)
		return
	}

	messages = 0
	quote := common.Quote{}
	err = p.Subscribe(os.Args[2], EventReceived, &quote)
	if err != nil {
		fmt.Printf("Erro ao se increver: %s\n", err)
		return
	}

	last_check := messages
	for {
		prior := last_check
		time.Sleep(time.Second)
		last_check = messages
		fmt.Printf("%d messages per second. %d messages received.\n", last_check-prior, last_check)
	}
}
