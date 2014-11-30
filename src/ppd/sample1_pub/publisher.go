package main

import (
	"fmt"
	"ppd/gpubsub"
	"os"
)

func main() {
	
	if len(os.Args) < 1 {
		fmt.Printf("Informe o topico.\n");
		return;
	}
	
	p := gpubsub.Publisher{}
	
	err := p.Connect("localhost:8999")
	if err != nil {
		fmt.Printf("Erro ao conectar: %s\n", err);
		return
	}

	for i := 0; i < 10000; i++ {
		err = p.Publish(os.Args[1], nil);
		if err != nil {
			fmt.Printf("Erro ao publicar: %s\n", err);
			return
		}
	}
	
	p.Disconnect();
		
}