package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"raft-groupX/miniraft"
)

// Server holds all the state for this Raft node
type Server struct {
	selfID string       // e.g. "localhost:2000"
	peers  []string     // all servers including self
	conn   *net.UDPConn // our UDP socket
}

func main() {
	if len(os.Args) != 3 {
		log.Fatal("Usage: go run raftserver/raftserver.go <host:port> <config-file>")
	}

	selfID := os.Args[1]
	configFile := os.Args[2]

	// Read config file
	peers, err := readConfig(configFile)
	if err != nil {
		log.Fatalf("Could not read config file: %v", err)
	}

	// Assert own identity is in the config
	found := false
	for _, p := range peers {
		if p == selfID {
			found = true
			break
		}
	}
	if !found {
		log.Fatalf("Own identity %s not found in config file", selfID)
	}

	log.Printf("Starting server %s, peers: %v", selfID, peers)

	// Bind to UDP port
	addr, err := net.ResolveUDPAddr("udp", selfID)
	if err != nil {
		log.Fatalf("Invalid address %s: %v", selfID, err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalf("Could not listen on %s: %v", selfID, err)
	}
	defer conn.Close()

	server := &Server{
		selfID: selfID,
		peers:  peers,
		conn:   conn,
	}

	log.Printf("Listening on %s", selfID)
	server.receiveLoop()
}

// receiveLoop reads incoming UDP packets forever
func (s *Server) receiveLoop() {
	buf := make([]byte, 1400)
	for {
		n, remoteAddr, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			log.Printf("Read error: %v", err)
			continue
		}

		// Copy the bytes out before the next read overwrites buf
		raw := make([]byte, n)
		copy(raw, buf[:n])

		go s.handleMessage(raw, remoteAddr)
	}
}

// handleMessage parses an incoming packet and dispatches it
func (s *Server) handleMessage(raw []byte, from *net.UDPAddr) {
	msg := &miniraft.RaftMessage{}
	msgType, err := msg.UnmarshalJSON(raw)
	if err != nil {
		log.Printf("Failed to parse message from %s: %v", from, err)
		return
	}

	switch msgType {
	case miniraft.AppendEntriesRequestMessage:
		req := msg.Message.(*miniraft.AppendEntriesRequest)
		log.Printf("AppendEntriesRequest from %s: term=%d leader=%s",
			from, req.Term, req.LeaderId)
		// TODO: handle in Milestone 3/4

	case miniraft.AppendEntriesResponseMessage:
		res := msg.Message.(*miniraft.AppendEntriesResponse)
		log.Printf("AppendEntriesResponse from %s: term=%d success=%v",
			from, res.Term, res.Success)
		// TODO: handle in Milestone 4

	case miniraft.RequestVoteRequestMessage:
		req := msg.Message.(*miniraft.RequestVoteRequest)
		log.Printf("RequestVoteRequest from %s: term=%d candidate=%s",
			from, req.Term, req.CandidateName)
		// TODO: handle in Milestone 3

	case miniraft.RequestVoteResponseMessage:
		res := msg.Message.(*miniraft.RequestVoteResponse)
		log.Printf("RequestVoteResponse from %s: term=%d granted=%v",
			from, res.Term, res.VoteGranted)
		// TODO: handle in Milestone 3

	default:
		log.Printf("Unknown message type from %s", from)
	}
}

// sendMessage serializes and sends a RaftMessage to a target address
func (s *Server) sendMessage(target string, payload any) error {
	msg := &miniraft.RaftMessage{Message: payload}
	data, err := msg.MarshalJson()
	if err != nil {
		return fmt.Errorf("marshal error: %v", err)
	}

	addr, err := net.ResolveUDPAddr("udp", target)
	if err != nil {
		return fmt.Errorf("resolve error for %s: %v", target, err)
	}

	_, err = s.conn.WriteTo(data, addr)
	if err != nil {
		return fmt.Errorf("send error to %s: %v", target, err)
	}
	return nil
}

// readConfig reads server identities from a config file, one per line
func readConfig(filename string) ([]string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var servers []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			servers = append(servers, line)
		}
	}
	return servers, scanner.Err()
}
