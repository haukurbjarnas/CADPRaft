package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"raft-groupX/miniraft"
)

// Server holds all the state for this Raft node

type State int

const (
	Follower State = iota
	Candidate
	Leader
	Failed
)

type Server struct {
	selfID string
	peers  []string
	conn   *net.UDPConn

	mu          sync.Mutex
	state       State
	currentTerm int
	votedFor    string
	voteCount   int
	leaderID    string

	// send any value here to reset the election timer
	resetElection chan struct{}
}

type ClientCommand struct {
	Command string
}

func (s *Server) printLog() {
	s.mu.Lock()
	defer s.mu.Unlock()
	// TODO: print log entries in Milestone 4
	log.Println("Log: (empty)")
}

func (s *Server) printState() {
	s.mu.Lock()
	defer s.mu.Unlock()
	// TODO: print full Raft state in Milestone 3/4
	log.Printf("State: %v", s.state)
}

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Failed:
		return "Failed"
	default:
		return "Unknown"
	}
}

func (s *Server) readStdin() {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		switch line {
		case "log":
			s.printLog()
		case "print":
			s.printState()
		case "suspend":
			s.mu.Lock()
			s.state = Failed
			s.mu.Unlock()
			log.Println("Server suspended")
		case "resume":
			s.mu.Lock()
			s.state = Follower
			s.mu.Unlock()
			// Drain any pending timer fires accumulated while suspended
			for {
				select {
				case <-s.resetElection:
				default:
					goto drained
				}
			}
		drained:
			// Now send a fresh reset to give heartbeats time to arrive
			s.resetElection <- struct{}{}
			log.Println("Server resumed")
		default:
			log.Printf("Unknown command: %s", line)
		}
	}
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
		selfID:        selfID,
		peers:         peers,
		conn:          conn,
		state:         Follower,
		currentTerm:   0,
		votedFor:      "",
		resetElection: make(chan struct{}, 10),
	}

	log.Printf("Listening on %s", selfID)
	go server.runElectionTimer()
	go server.readStdin()
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
	s.mu.Lock()
	state := s.state
	s.mu.Unlock()

	if state == Failed {
		log.Printf("Suspended, ignoring message from %s", from)
		return

	} else if bytes.Contains(raw, []byte(`"LeaderId"`)) {
		req := &miniraft.AppendEntriesRequest{}
		if err := json.Unmarshal(raw, req); err != nil {
			log.Printf("Failed to parse AppendEntriesRequest from %s: %v", from, err)
			return
		}
		go s.handleAppendEntries(req, from)

	} else if bytes.Contains(raw, []byte(`"CandidateName"`)) {
		req := &miniraft.RequestVoteRequest{}
		if err := json.Unmarshal(raw, req); err != nil {
			log.Printf("Failed to parse RequestVoteRequest from %s: %v", from, err)
			return
		}
		go s.handleRequestVote(req, from)

	} else if bytes.Contains(raw, []byte(`"Success"`)) {
		res := &miniraft.AppendEntriesResponse{}
		if err := json.Unmarshal(raw, res); err != nil {
			log.Printf("Failed to parse AppendEntriesResponse from %s: %v", from, err)
			return
		}
		log.Printf("AppendEntriesResponse from %s: term=%d success=%v", from, res.Term, res.Success)
		// TODO: handle in Milestone 4

	} else if bytes.Contains(raw, []byte(`"VoteGranted"`)) {
		res := &miniraft.RequestVoteResponse{}
		if err := json.Unmarshal(raw, res); err != nil {
			log.Printf("Failed to parse RequestVoteResponse from %s: %v", from, err)
			return
		}
		go s.handleRequestVoteResponse(res, from)

	} else if bytes.Contains(raw, []byte(`"Command"`)) {
		cmd := &ClientCommand{}
		if err := json.Unmarshal(raw, cmd); err != nil {
			log.Printf("Failed to parse client command from %s: %v", from, err)
			return
		}
		log.Printf("Client command from %s: %s", from, cmd.Command)
		// TODO Milestone 4: leader appends to log, followers forward to leader

	} else {
		log.Printf("Unknown message type from %s: %s", from, raw)
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

func (s *Server) runElectionTimer() {
	for {
		// Check state at the start of each loop iteration
		s.mu.Lock()
		state := s.state
		s.mu.Unlock()

		// If Failed, wait a bit and check again — don't start timing
		if state == Failed {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		timeout := time.Duration(300+rand.Intn(300)) * time.Millisecond

		select {
		case <-s.resetElection:
			// heartbeat or vote received — reset the timer
		case <-time.After(timeout):
			s.mu.Lock()
			state := s.state
			s.mu.Unlock()

			if state == Follower {
				go s.startElection()
			}
		}
	}
}

func (s *Server) startElection() {

	s.mu.Lock()
	if s.state == Failed || s.state == Leader || s.state == Candidate {
		s.mu.Unlock()
		return
	}

	s.state = Candidate
	s.currentTerm++
	s.votedFor = s.selfID
	s.voteCount = 1
	term := s.currentTerm
	lastLogIndex, lastLogTerm := s.lastLogInfo()
	s.mu.Unlock()

	log.Printf("Starting election for term %d", term)

	for _, peer := range s.peers {
		if peer == s.selfID {
			continue
		}
		go func(target string) {
			req := &miniraft.RequestVoteRequest{
				Term:          term,
				CandidateName: s.selfID,
				LastLogIndex:  lastLogIndex,
				LastLogTerm:   lastLogTerm,
			}
			if err := s.sendMessage(target, req); err != nil {
				log.Printf("Failed to send RequestVote to %s: %v", target, err)
			}
		}(peer)
	}

	// Wait for election timeout — if we haven't won by then, start a new one
	// This replaces the timer goroutine re-triggering us
	timeout := time.Duration(300+rand.Intn(300)) * time.Millisecond
	time.Sleep(timeout)

	s.mu.Lock()
	// If we're still a Candidate after the timeout, start a new election
	stillCandidate := s.state == Candidate
	s.mu.Unlock()

	if stillCandidate {
		go s.startElection()
	}
}

func (s *Server) lastLogInfo() (int, int) {
	// TODO: will use actual log in Milestone 4
	// For now return 0, 0 (empty log)
	return 0, 0
}

func (s *Server) handleRequestVote(req *miniraft.RequestVoteRequest, from *net.UDPAddr) {
	s.mu.Lock()
	defer s.mu.Unlock()

	response := &miniraft.RequestVoteResponse{
		Term:        s.currentTerm,
		VoteGranted: false,
	}

	// Rule 1 from Figure 2: reply false if term < currentTerm
	if req.Term < s.currentTerm {
		s.sendMessage(from.String(), response)
		return
	}

	// If we see a higher term, update and revert to follower
	if req.Term > s.currentTerm {
		s.currentTerm = req.Term
		s.state = Follower
		s.votedFor = ""
	}

	// Rule 2: grant vote if votedFor is null or candidateId,
	// AND candidate's log is at least as up-to-date as ours
	lastIndex, lastTerm := s.lastLogInfo()
	logOK := req.LastLogTerm > lastTerm ||
		(req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIndex)

	if (s.votedFor == "" || s.votedFor == req.CandidateName) && logOK {
		s.votedFor = req.CandidateName
		response.VoteGranted = true
		response.Term = s.currentTerm

		// Reset election timer since we just granted a vote
		select {
		case s.resetElection <- struct{}{}:
		default:
		}
	}

	log.Printf("Vote request from %s term=%d: granted=%v",
		req.CandidateName, req.Term, response.VoteGranted)

	s.sendMessage(from.String(), response)
}

func (s *Server) handleRequestVoteResponse(res *miniraft.RequestVoteResponse, from *net.UDPAddr) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// If we see a higher term, step down
	if res.Term > s.currentTerm {
		s.currentTerm = res.Term
		s.state = Follower
		s.votedFor = ""
		return
	}

	// Only count votes if we're still a Candidate
	if s.state != Candidate {
		return
	}

	if res.VoteGranted {
		s.voteCount++
		log.Printf("Got vote, total=%d needed=%d", s.voteCount, s.majority())

		// Only transition once — check we haven't already become leader
		if s.voteCount == s.majority() { // == not >= so it only fires exactly once
			go s.becomeLeader()
		}
	}
}

func (s *Server) majority() int {
	return len(s.peers)/2 + 1
}

func (s *Server) becomeLeader() {
	s.mu.Lock()
	// Guard: if we're no longer a Candidate, don't become leader
	if s.state != Candidate {
		s.mu.Unlock()
		return
	}
	s.state = Leader
	s.leaderID = s.selfID
	term := s.currentTerm
	s.mu.Unlock()

	log.Printf("Became leader for term %d!", term)
	go s.runHeartbeat()
}

func (s *Server) runHeartbeat() {
	for {
		s.mu.Lock()
		state := s.state
		term := s.currentTerm
		s.mu.Unlock()

		// Stop sending heartbeats if we're no longer leader
		if state != Leader {
			return
		}

		// Send empty AppendEntries to all peers as heartbeat
		for _, peer := range s.peers {
			if peer == s.selfID {
				continue
			}
			go func(target string) {
				req := &miniraft.AppendEntriesRequest{
					Term:         term,
					LeaderId:     s.selfID,
					LeaderCommit: 0, // will update in Milestone 4
				}
				if err := s.sendMessage(target, req); err != nil {
					log.Printf("Failed to send heartbeat to %s: %v", target, err)
				}
			}(peer)
		}

		// Heartbeat interval should be well below election timeout
		// Paper recommends broadcastTime << electionTimeout
		// We use 75ms (half of minimum election timeout of 150ms)
		time.Sleep(75 * time.Millisecond)
	}
}

func (s *Server) handleAppendEntries(req *miniraft.AppendEntriesRequest, from *net.UDPAddr) {
	s.mu.Lock()
	defer s.mu.Unlock()

	response := &miniraft.AppendEntriesResponse{
		Term:    s.currentTerm,
		Success: false,
	}

	// Rule 1: reply false if term < currentTerm
	if req.Term < s.currentTerm {
		s.sendMessage(from.String(), response)
		return
	}

	// Valid leader contact — update term and revert to follower if needed
	if req.Term > s.currentTerm {
		s.currentTerm = req.Term
		s.votedFor = ""
	}
	s.state = Follower
	s.leaderID = req.LeaderId

	// Reset election timer — we heard from a valid leader
	select {
	case s.resetElection <- struct{}{}:
	default:
	}

	response.Success = true
	response.Term = s.currentTerm

	// TODO Milestone 4: log consistency check goes here

	s.sendMessage(from.String(), response)
}
