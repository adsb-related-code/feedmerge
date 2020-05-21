package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/paulbellamy/ratecounter"
	"github.com/tv42/topic"
)

var countInbound = 0
var countOutbound = 0
var countMessagesInbound = ratecounter.NewRateCounter(60 * time.Second)
var countMessagesOutbound = ratecounter.NewRateCounter(60 * time.Second)

func main() {
	var inboundPort int
	var outboundPort int
	var outUrl string

	flag.IntVar(&inboundPort, "inport", 2000, "Port to listen for inbound TCP/UDP feeds")
	flag.IntVar(&outboundPort, "outport", 3000, "Port to provide aggregated TCP feed")
	flag.StringVar(&outUrl, "outurl", "", "Send data out with gusto.")

	var msgsize int
	flag.IntVar(&msgsize, "msgsize", 4096, "Maximum message size")

	flag.Parse()

	if len(os.Args) < 2 {
		flag.PrintDefaults()
		os.Exit(1)
	}

	top := topic.New()
	go startInboundTCP(inboundPort, top, msgsize)
	go startInboundUDP(inboundPort, top, msgsize)
	if outUrl != "" {
		go startSendTCP(outUrl, top, msgsize)
	}
	go startOutboundTCP(outboundPort, top)

	for {
		time.Sleep(60 * time.Second)
		msgSecIn := countMessagesInbound.Rate() / 60
		msgSecOut := countMessagesOutbound.Rate() / 60
		fmt.Printf("Inbound: %v clients, %v msg/sec. Outbound: %v clients, %v msg/sec\n",
			countInbound, msgSecIn, countOutbound, msgSecOut)
	}
}

func startSendTCP(outURL string, top *topic.Topic, msgsize int) {
	conn, err := net.Dial("tcp", outURL)
	if err != nil {
	} else {
		fmt.Println("TCP send connection.")
		go handleOutbound(conn, top)
	}
}

func startInboundUDP(inboundPort int, top *topic.Topic, msgsize int) {
	pc, err := net.ListenPacket("udp", "0.0.0.0:"+strconv.Itoa(inboundPort))
	if err != nil {
		fmt.Println("Error binding UDP: ", err.Error())
		os.Exit(1)
	}
	defer pc.Close()
	buf := make([]byte, msgsize)
	for {
		_, _, err = pc.ReadFrom(buf)
		if err != nil {
			fmt.Println("Error reading from UDP: ", err.Error())
			return
		}
		countMessagesInbound.Incr(1)
		top.Broadcast <- buf
	}
}

func startInboundTCP(inboundPort int, top *topic.Topic, msgsize int) {
	l, err := net.Listen("tcp", "0.0.0.0:"+strconv.Itoa(inboundPort))
	if err != nil {
		fmt.Println("Error binding TCP: ", err.Error())
		os.Exit(1)
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		go handleInbound(conn, top, msgsize)
	}
}

func startOutboundTCP(outboundPort int, top *topic.Topic) {
	l, err := net.Listen("tcp", "0.0.0.0:"+strconv.Itoa(outboundPort))
	if err != nil {
		fmt.Println("Error binding port: ", err.Error())
		os.Exit(1)
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		go handleOutbound(conn, top)
	}
}

func handleInboundExit() {
	countInbound--
}

func handleOutboundExit() {
	countOutbound--
}

func handleInbound(conn net.Conn, top *topic.Topic, msgsize int) {
	defer conn.Close()
	defer handleInboundExit()
	countInbound++
	for {
		buf := make([]byte, msgsize)
		_, err := conn.Read(buf)
		if err != nil {
			conn.Close()
			return
		}
		countMessagesInbound.Incr(1)
		top.Broadcast <- buf
	}
}

func handleOutbound(conn net.Conn, top *topic.Topic) {
	defer conn.Close()
	defer handleOutboundExit()
	countOutbound++

	consumer := make(chan interface{}, 10000)
	defer top.Unregister(consumer)
	top.Register(consumer)

	for msg := range consumer {
		_, err := conn.Write(msg.([]byte))
		if err != nil {
			conn.Close()
			top.Unregister(consumer)
			return
		}
		countMessagesOutbound.Incr(1)
	}
	fmt.Println("Not OK!")
}
