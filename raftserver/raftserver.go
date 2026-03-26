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

	"raft-group5/miniraft"
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
	resumedAt   time.Time

	logFileReady bool

	resetElection chan struct{}

	// Raft log
	log []miniraft.LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders only
	nextIndex  map[string]int
	matchIndex map[string]int
}

type ClientCommand struct {
	Command string
}

func (s *Server) printLog() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.log) == 0 {
		log.Println("Log: (empty)")
		return
	}
	for _, entry := range s.log {
		log.Printf("  %d,%d,%s", entry.Term, entry.Index, entry.CommandName)
	}
}

func (s *Server) printState() {
	s.mu.Lock()
	defer s.mu.Unlock()
	log.Printf("State: %v | Term: %d | VotedFor: %s | Leader: %s | commitIndex: %d | lastApplied: %d",
		s.state, s.currentTerm, s.votedFor, s.leaderID, s.commitIndex, s.lastApplied)
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
		resumedAt:     time.Now(),
		log:           []miniraft.LogEntry{},
		commitIndex:   0,
		lastApplied:   0,
		nextIndex:     make(map[string]int),
		matchIndex:    make(map[string]int),
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
		go s.handleAppendEntriesResponse(res, from)

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
		go s.handleClientCommand(cmd.Command, from)
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

// runElectionTimer implements the Follower election timeout rule from Figure 2:
// "If election timeout elapses without receiving AppendEntries RPC from current
// leader or granting vote to candidate: convert to candidate" (§5.2)
// Randomized timeout of 300-600ms satisfies broadcastTime << electionTimeout (§5.6)
func (s *Server) runElectionTimer() {
	for {
		// Check state at the start of each loop iteration
		s.mu.Lock()
		state := s.state
		s.mu.Unlock()

		// If Failed, wait a bit and check again
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

// startElection implements the Candidate election rules from Figure 2 (§5.2):
// "On conversion to candidate, start election:
//   - Increment currentTerm
//   - Vote for self
//   - Reset election timer
//   - Send RequestVote RPCs to all other servers"
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

	// Wait for election timeout — if haven't won by then, start a new one
	timeout := time.Duration(300+rand.Intn(300)) * time.Millisecond
	time.Sleep(timeout)

	s.mu.Lock()
	// If still a Candidate after the timeout, start a new election
	stillCandidate := s.state == Candidate
	s.mu.Unlock()

	if stillCandidate {
		go s.startElection()
	}
}

func (s *Server) lastLogInfo() (int, int) {
	if len(s.log) == 0 {
		return 0, 0
	}
	last := s.log[len(s.log)-1]
	return last.Index, last.Term
}

// handleRequestVote implements the RequestVote RPC receiver from Figure 2 (§5.2, §5.4):
// Rule 1: Reply false if term < currentTerm
// Rule 2: If votedFor is null or candidateId, and candidate's log is at least
//         as up-to-date as receiver's log, grant vote
func (s *Server) handleRequestVote(req *miniraft.RequestVoteRequest, from *net.UDPAddr) {
	s.mu.Lock()
	defer s.mu.Unlock()

	response := &miniraft.RequestVoteResponse{
		Term:        s.currentTerm,
		VoteGranted: false,
	}

	// Rule 1
	if req.Term < s.currentTerm {
		s.sendMessage(from.String(), response)
		return
	}

	if req.Term > s.currentTerm {
		s.currentTerm = req.Term
		s.state = Follower
		s.votedFor = ""
	}

	// Rule 2
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

// handleRequestVoteResponse implements the Candidate vote-counting rule from Figure 2 (§5.2):
// "If votes received from majority of servers: become leader"
// Also implements All Servers rule: step down if response contains higher term (§5.1)
func (s *Server) handleRequestVoteResponse(res *miniraft.RequestVoteResponse, from *net.UDPAddr) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// If see a higher term, step down
	if res.Term > s.currentTerm {
		s.currentTerm = res.Term
		s.state = Follower
		s.votedFor = ""
		return
	}

	// Only count votes if still a Candidate
	if s.state != Candidate {
		return
	}

	if res.VoteGranted {
		s.voteCount++
		log.Printf("Got vote, total=%d needed=%d", s.voteCount, s.majority())

		// Only transition once 
		if s.voteCount == s.majority() {
			go s.becomeLeader()
		}
	}
}

func (s *Server) majority() int {
	return len(s.peers)/2 + 1
}

// becomeLeader implements the Leader initialization rules from Figure 2 (§5.2, §5.3):
// "Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server"
// Reinitializes nextIndex[] and matchIndex[] for all peers after election
func (s *Server) becomeLeader() {
	s.mu.Lock()
	if s.state != Candidate {
		s.mu.Unlock()
		return
	}
	s.state = Leader
	s.leaderID = s.selfID
	term := s.currentTerm

	// Initialize nextIndex and matchIndex for all peers
	lastIndex, _ := s.lastLogInfo()
	for _, peer := range s.peers {
		if peer == s.selfID {
			continue
		}
		s.nextIndex[peer] = lastIndex + 1
		s.matchIndex[peer] = 0
	}
	s.mu.Unlock()

	log.Printf("Became leader for term %d!", term)
	go s.runHeartbeat()
}

// runHeartbeat implements the Leader heartbeat rule from Figure 2 (§5.2):
// "repeat during idle periods to prevent election timeouts"
// Heartbeat interval of 75ms satisfies broadcastTime << electionTimeout (§5.6)
// Also serves as log replication trigger per §5.3
func (s *Server) runHeartbeat() {
	for {
		s.mu.Lock()
		state := s.state
		s.mu.Unlock()

		if state != Leader {
			return
		}

		// Heartbeat doubles as log replication
		s.replicateLog()

		time.Sleep(75 * time.Millisecond)
	}
}

// handleAppendEntries implements the AppendEntries RPC receiver from Figure 2 (§5.3):
// Rule 1: Reply false if term < currentTerm
// Rule 2: Reply false if log doesn't contain entry at prevLogIndex matching prevLogTerm
// Rule 3: If existing entry conflicts with new one, delete it and all that follow
// Rule 4: Append any new entries not already in the log
// Rule 5: If leaderCommit > commitIndex, update commitIndex
// Also implements Follower rule: reset election timer on valid AppendEntries (§5.2)
func (s *Server) handleAppendEntries(req *miniraft.AppendEntriesRequest, from *net.UDPAddr) {
	s.mu.Lock()
	defer s.mu.Unlock()

	response := &miniraft.AppendEntriesResponse{
		Term:    s.currentTerm,
		Success: false,
	}

	// Rule 1
	if req.Term < s.currentTerm {
		s.sendMessage(from.String(), response)
		return
	}

	if req.Term > s.currentTerm {
		s.currentTerm = req.Term
		s.votedFor = ""
	}
	s.state = Follower
	s.leaderID = req.LeaderId

	// Reset election timer
	select {
	case s.resetElection <- struct{}{}:
	default:
	}

	// Rule 2
	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex > len(s.log) {
			s.sendMessage(from.String(), response)
			return
		}
		if s.log[req.PrevLogIndex-1].Term != req.PrevLogTerm {
			s.sendMessage(from.String(), response)
			return
		}
	}

	// Rule 3 & 4: handle incoming log entries
	for i, entry := range req.LogEntries {
		idx := req.PrevLogIndex + i + 1
		if idx <= len(s.log) {
			if s.log[idx-1].Term != entry.Term {
				s.log = s.log[:idx-1]
				s.log = append(s.log, entry)
			}
		} else {
			// New entry — append
			s.log = append(s.log, entry)
		}
	}

	// Rule 5
	if req.LeaderCommit > s.commitIndex {
		lastNewIndex := req.PrevLogIndex + len(req.LogEntries)
		if req.LeaderCommit < lastNewIndex {
			s.commitIndex = req.LeaderCommit
		} else {
			s.commitIndex = lastNewIndex
		}
		go s.applyCommitted()
	}

	response.Success = true
	response.Term = s.currentTerm
	s.sendMessage(from.String(), response)
}

// handleClientCommand implements the Leader client command rule from Figure 2 (§5.3):
// "If command received from client: append entry to local log"
// Followers forward to leader per assignment spec (§5.1 follower redirection)
func (s *Server) handleClientCommand(command string, from *net.UDPAddr) {
	s.mu.Lock()

	if s.state == Leader {
		// Append to own log
		lastIndex, _ := s.lastLogInfo()
		entry := miniraft.LogEntry{
			Index:       lastIndex + 1,
			Term:        s.currentTerm,
			CommandName: command,
		}
		s.log = append(s.log, entry)
		log.Printf("Leader appended entry: index=%d term=%d cmd=%s",
			entry.Index, entry.Term, entry.CommandName)
		s.mu.Unlock()

		// Replicate to all followers
		s.replicateLog()

	} else if s.state == Follower && s.leaderID != "" {
		// Forward to leader
		log.Printf("Forwarding command %s to leader %s", command, s.leaderID)
		s.mu.Unlock()
		cmd := &ClientCommand{Command: command}
		if err := s.sendMessage(s.leaderID, cmd); err != nil {
			log.Printf("Failed to forward command to leader: %v", err)
		}
	} else {
		// Candidate or no known leader — drop it
		log.Printf("No leader known, dropping command: %s", command)
		s.mu.Unlock()
	}
}

// replicateLog implements the Leader log replication rule from Figure 2 (§5.3):
// "If last log index >= nextIndex for a follower: send AppendEntries RPC
//  with log entries starting at nextIndex"
func (s *Server) replicateLog() {
	s.mu.Lock()
	term := s.currentTerm
	commitIndex := s.commitIndex
	peers := s.peers
	s.mu.Unlock()

	for _, peer := range peers {
		if peer == s.selfID {
			continue
		}
		go func(target string) {
			s.mu.Lock()

			// Only replicate if still leader
			if s.state != Leader {
				s.mu.Unlock()
				return
			}

			nextIdx := s.nextIndex[target]
			prevLogIndex := nextIdx - 1
			prevLogTerm := 0

			// Find the term of the previous log entry
			if prevLogIndex > 0 && prevLogIndex <= len(s.log) {
				prevLogTerm = s.log[prevLogIndex-1].Term
			}

			// Entries to send — everything from nextIndex onwards
			entries := []miniraft.LogEntry{}
			if nextIdx <= len(s.log) {
				entries = s.log[nextIdx-1:]
			}

			req := &miniraft.AppendEntriesRequest{
				Term:         term,
				LeaderId:     s.selfID,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				LogEntries:   entries,
				LeaderCommit: commitIndex,
			}
			s.mu.Unlock()

			if err := s.sendMessage(target, req); err != nil {
				log.Printf("Failed to send AppendEntries to %s: %v", target, err)
			}
		}(peer)
	}
}

// handleAppendEntriesResponse implements the Leader response handling rules from Figure 2 (§5.3):
// On success: "update nextIndex and matchIndex for follower"
// On failure: "decrement nextIndex and retry"
// Also implements All Servers rule: step down if response contains higher term (§5.1)
func (s *Server) handleAppendEntriesResponse(res *miniraft.AppendEntriesResponse, from *net.UDPAddr) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if res.Term > s.currentTerm {
		s.currentTerm = res.Term
		s.state = Follower
		s.votedFor = ""
		return
	}

	if s.state != Leader {
		return
	}

	target := s.resolvePeer(from)
	if target == "" {
		return
	}

	if res.Success {
		// Follower's log now matches ours up to nextIndex-1
		// Advance matchIndex to the last entry we could have sent
		lastIndex := 0
		if len(s.log) > 0 {
			lastIndex = s.log[len(s.log)-1].Index
		}
		if lastIndex > s.matchIndex[target] {
			s.matchIndex[target] = lastIndex
			s.nextIndex[target] = lastIndex + 1
		}
		s.advanceCommitIndex()
	} else {
		// Log inconsistency — back off
		if s.nextIndex[target] > 1 {
			s.nextIndex[target]--
		}
	}
}

// resolvePeer converts a UDP address back to a peer identity string
func (s *Server) resolvePeer(from *net.UDPAddr) string {
	for _, peer := range s.peers {
		addr, err := net.ResolveUDPAddr("udp", peer)
		if err != nil {
			continue
		}
		if addr.Port == from.Port {
			return peer
		}
	}
	return ""
}

// advanceCommitIndex implements the Leader commit rule from Figure 2 (§5.3, §5.4):
// "If there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N,
//  and log[N].term == currentTerm: set commitIndex = N"
// The currentTerm check is critical per §5.4.2 — prevents unsafe commits of old entries
func (s *Server) advanceCommitIndex() {
	// Find the highest N where a majority have matchIndex >= N
	// and log[N].term == currentTerm
	for n := len(s.log); n > s.commitIndex; n-- {
		if s.log[n-1].Term != s.currentTerm {
			continue
		}
		// Count how many servers have this entry
		count := 1 // count self
		for _, peer := range s.peers {
			if peer == s.selfID {
				continue
			}
			if s.matchIndex[peer] >= n {
				count++
			}
		}
		if count >= s.majority() {
			s.commitIndex = n
			log.Printf("Advanced commitIndex to %d", s.commitIndex)
			go s.applyCommitted()
			break
		}
	}
}

// applyCommitted implements the All Servers state machine rule from Figure 2 (§5.3):
// "If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied]
//  to state machine"
// Writing to the .log file serves as our state machine application
func (s *Server) applyCommitted() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for s.lastApplied < s.commitIndex {
		s.lastApplied++
		entry := s.log[s.lastApplied-1]

		// Write to log file: term,index,command
		s.writeToLogFile(entry)
		log.Printf("Applied entry: index=%d term=%d cmd=%s",
			entry.Index, entry.Term, entry.CommandName)
	}
}

func (s *Server) writeToLogFile(entry miniraft.LogEntry) {
	filename := strings.ReplaceAll(s.selfID, ":", "-") + ".log"

	// First write: truncate the file to start fresh
	// Subsequent writes: append
	flag := os.O_APPEND | os.O_CREATE | os.O_WRONLY
	if !s.logFileReady {
		flag = os.O_TRUNC | os.O_CREATE | os.O_WRONLY
		s.logFileReady = true
	}

	f, err := os.OpenFile(filename, flag, 0644)
	if err != nil {
		log.Printf("Failed to open log file: %v", err)
		return
	}
	defer f.Close()

	line := fmt.Sprintf("%d,%d,%s\n", entry.Term, entry.Index, entry.CommandName)
	if _, err := f.WriteString(line); err != nil {
		log.Printf("Failed to write to log file: %v", err)
	}
}
