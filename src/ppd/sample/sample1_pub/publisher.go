package main

import (
	"fmt"
	"ppd/gpubsub"
	"os"
	"ppd/sample/common"
	"math/rand"
	"time"
	"encoding/json"
)

func main() {
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
		quote.SetQuote(r.Float64())
		for i := 0; i < len(quote.Others); i++ {
			quote.Others[i] = r.Float64();
		}

		ebytes, _ := json.Marshal(quote);
		fmt.Printf("Event size: %d bytes\n", len(ebytes));
			
		for {
			quote.SetQuote(r.Float64())
			for i := 0; i < len(quote.Others); i++ {
				quote.Others[i] = r.Float64();
			}
			//fmt.Printf("Publishing quote %f %f\n", quote.GetQuote())
		
			err = p.Publish(os.Args[2], quote)
			if err != nil {
				fmt.Printf("Erro ao publicar: %s\n", err)
				return
			}
			sent++;
		}
	}()
	
	last_check := sent;
	for {
		prior := last_check;
		time.Sleep(time.Second);
		last_check = sent;
		fmt.Printf("%d messages per second. %d messages sent.\n", last_check-prior, last_check);
	}		
	p.Disconnect();
		
}