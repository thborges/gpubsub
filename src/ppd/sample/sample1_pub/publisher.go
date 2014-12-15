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
	
	quote := common.Quote{}
	for i := 0; i < 10000; i++ {
		
		quote.SetQuote(r.Float64())
		//fmt.Printf("Publishing quote %f %f\n", quote.GetQuote())
		
		err = p.Publish(os.Args[2], quote)
		if err != nil {
			fmt.Printf("Erro ao publicar: %s\n", err)
			return
		}
	}
	
	p.Disconnect();
		
}