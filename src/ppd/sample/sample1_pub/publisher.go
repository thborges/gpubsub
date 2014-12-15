package main

import (
	"fmt"
	"ppd/gpubsub"
	"os"
	"ppd/sample/common"
	"math/rand"
	"time"
	"encoding/gob"
)

func main() {
	gob.Register(common.Quote{})
	
	if len(os.Args) < 2 {
		fmt.Printf("Informe o servidor e o tópico.\n")
		return;
	}
	
	p := gpubsub.Publisher{}
	
	err := p.Connect(os.Args[1])
	if err != nil {
		fmt.Printf("Erro ao conectar em %s: %s\n", os.Args[1], err)
		return
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	
	sent := 0;
	
	go func() {	
		quote := common.Quote{}
		for {
			quote.SetQuote(r.Float64())
			//fmt.Printf("Publishing quote %f %f\n", quote.GetQuote())
		
			err = p.Publish(os.Args[2], quote)
			if err != nil {
				fmt.Printf("Erro ao publicar: %s\n", err)
				return
			}
			sent++;
		}
	}()
	
	for {
		mymess := sent;
		time.Sleep(time.Second);
		fmt.Printf("%d messages per second. %d messages sent.\n", sent-mymess, sent);
	}
	
	p.Disconnect();
		
}