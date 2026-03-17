package miniraft

import "encoding/json"

type LogEntry struct {
	Index       int
	Term        int
	CommandName string
}

type AppendEntriesRequest struct {
	Term         int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	LeaderId     string
	LogEntries   []LogEntry
}

type AppendEntriesResponse struct {
	Term    int
	Success bool
}

type RequestVoteRequest struct {
	Term          int
	LastLogIndex  int
	LastLogTerm   int
	CandidateName string
}

type RequestVoteResponse struct {
	Term        int
	VoteGranted bool
}

type MessageType int

const (
	AppendEntriesRequestMessage MessageType = iota
	AppendEntriesResponseMessage
	RequestVoteRequestMessage
	RequestVoteResponseMessage
)

type RaftMessage struct {
	Message any
}

func (message *RaftMessage) MarshalJson() (result []byte, err error) {
	result, err = json.Marshal(message.Message)
	return
}

func (message *RaftMessage) UnmarshalJSON(b []byte) (msg MessageType, err error) {
	aer := &AppendEntriesRequest{}
	err = json.Unmarshal(b, aer)
	if err == nil && aer.LeaderId != "" {
		msg = AppendEntriesRequestMessage
		message.Message = aer
		return
	}

	if _, ok := err.(*json.UnmarshalTypeError); err != nil && !ok {
		return
	}

	aeres := &AppendEntriesResponse{}
	if err = json.Unmarshal(b, aeres); err != nil {
		return
	}
	if aeres.Term > 0 {
		msg = AppendEntriesResponseMessage
		message.Message = aeres
		return
	}

	rvr := &RequestVoteRequest{}
	if err = json.Unmarshal(b, rvr); err != nil {
		return
	}
	if rvr.Term > 0 {
		msg = RequestVoteRequestMessage
		message.Message = rvr
		return
	}

	rvres := &RequestVoteResponse{}
	if err = json.Unmarshal(b, rvres); err != nil {
		return
	}
	if rvres.Term > 0 {
		msg = RequestVoteResponseMessage
		message.Message = rvres
	}
	return
}
