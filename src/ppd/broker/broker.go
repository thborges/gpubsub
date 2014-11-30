package main

import (
	"fmt"
	"ppd/gpubsub"
	//"time"
)

func main() {
	b := gpubsub.Broker{}
	port := ":8999"
	fmt.Printf("Server listening on %s\n", port);
	
	/*go func(b *gpubsub.Broker) {
		for {
			time.Sleep(5*time.Second)
			b.PrintTopics();
		}	
	}(&b)*/
	
	b.Start(port, 10)

}