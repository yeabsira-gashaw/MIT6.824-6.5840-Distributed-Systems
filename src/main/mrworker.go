package main

//
// start a worker process, which is implemented
// in ../mr/worker.go. typically there will be
// multiple worker processes, talking to one coordinator.
//
// go run mrworker.go wc.so
//
// Please do not change this file.
//

import (
	"net"
	"net/rpc"
	"plugin"
	"time"

	"6.5840/mr"
)
import "os"
import "fmt"
import "log"

type Workers struct {
	ID      string
	RPCAddr string
	Port    string
}

func (w *Workers) Ping(args *mr.PingArgs, reply *mr.PingReply) error {

	reply.PingStatus = args.WorkerId == w.ID
	return nil
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so\n")
		os.Exit(1)
	}

	worker := new(Workers)
	err := rpc.Register(worker)
	if err != nil {
		log.Fatal("RPC register error:", err)
	}

	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal("listen error:", err)
	}

	defer listener.Close()

	addr := listener.Addr().(*net.TCPAddr)
	fmt.Printf("Worker listening on %s:%d\n", addr.IP, addr.Port)

	worker.ID = fmt.Sprintf("worker-%d", time.Now().UnixMicro())
	worker.RPCAddr = addr.IP.String()
	worker.Port = fmt.Sprintf("%d", addr.Port)

	go mr.WorkerRegistrationRequest(worker.ID, addr)

	rpc.Accept(listener) //allow incoming requests

	//mapf, reducef := loadPlugin(os.Args[1])

	//mr.Worker(mapf, reducef)

}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
