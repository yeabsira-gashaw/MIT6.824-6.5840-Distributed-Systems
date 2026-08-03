package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"log"
	"time"

	"6.5840/mr"
)
import "os"
import "fmt"

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: coordinator input-files... %v \n", os.Stderr)
		os.Exit(1)
	}

	err := os.MkdirAll("mr-data/intermediate", 0755)
	if err != nil {
		log.Fatal("Error creating intermediate directory")
	}

	err = os.MkdirAll("mr-data/output", 0755)
	if err != nil {
		log.Fatal("Error creating output directory")
	}

	m := mr.MakeCoordinator(os.Args[1:], 10)
	currentPhase := m.Done()
	for currentPhase != mr.FinishedPhase {
		fmt.Printf("%d Workers running , Current Phase : %d \n", m.ActiveWorkers(), currentPhase)
		time.Sleep(10 * time.Second)
	}

	fmt.Println("All jobs finished.")
}
