package main

import (
	"log"
	"net"
)

func main() {
	target, err := net.ResolveUDPAddr("udp", "localhost:2001")
	if err != nil {
		log.Fatal(err)
	}

	conn, err := net.ListenPacket("udp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	msg := []byte(`{"Term":1,"LastLogIndex":0,"LastLogTerm":0,"CandidateName":"localhost:2001"}`)

	_, err = conn.WriteTo(msg, target)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Sent test message to localhost:2000")
}
