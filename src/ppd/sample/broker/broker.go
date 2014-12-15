package main

import (
	"fmt"
	"ppd/gpubsub"
	"os"
	"strconv"
)

func main() {
	
	if len(os.Args) < 2 {
		fmt.Printf("Informe a porta e o tamanho do buffer em cada topico.\n");
		return;
	}
		
	b := gpubsub.Broker{}
	port := os.Args[1]
	fmt.Printf("Server listening on %s\n", port);
	
	/*go func(b *gpubsub.Broker) {
		for {
			time.Sleep(5*time.Second)
			b.PrintTopics();
		}	
	}(&b)*/
	
	k, _ := strconv.Atoi(os.Args[2])
	b.Start(port, k, 20)

}