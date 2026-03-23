
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
)

// ClientCommand is the message format the client sends to the server
type ClientCommand struct {
	Command string
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Usage: go run raftclient/raftclient.go <server-host:port>")
	}

	serverAddr := os.Args[1]

	// Resolve the server address
	target, err := net.ResolveUDPAddr("udp", serverAddr)
	if err != nil {
		log.Fatalf("Invalid server address %s: %v", serverAddr, err)
	}

	// Bind to a local port to send from
	conn, err := net.ListenPacket("udp", ":0") // :0 = pick any free port
	if err != nil {
		log.Fatalf("Could not open UDP socket: %v", err)
	}
	defer conn.Close()

	log.Printf("Client connected to server %s", serverAddr)
	fmt.Println("Type a command and press Enter. Type 'exit' to quit.")

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if line == "" {
			continue
		}

		if line == "exit" {
			log.Println("Exiting.")
			return
		}

		// Validate: only letters, digits, dashes, underscores allowed
		if !isValidCommand(line) {
			fmt.Println("Invalid command: only letters, digits, dashes, and underscores allowed")
			continue
		}

		// Serialize to JSON
		msg := ClientCommand{Command: line}
		data, err := json.Marshal(msg)
		if err != nil {
			log.Printf("Failed to serialize command: %v", err)
			continue
		}

		// Send to server
		_, err = conn.WriteTo(data, target)
		if err != nil {
			log.Printf("Failed to send command: %v", err)
			continue
		}

		log.Printf("Sent command: %s", line)
	}
}

// isValidCommand checks that the command only contains
// letters, digits, dashes, and underscores
func isValidCommand(s string) bool {
	for _, c := range s {
		if !('a' <= c && c <= 'z') &&
			!('A' <= c && c <= 'Z') &&
			!('0' <= c && c <= '9') &&
			c != '-' && c != '_' {
			return false
		}
	}
	return true
}