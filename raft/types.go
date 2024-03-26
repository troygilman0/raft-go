package raft

type serverInfo struct {
	id         string
	nextIndex  uint
	matchIndex uint
}

type State uint

const (
	StateLeader State = iota
	StateFollower
	StateCandidate
)

type LogEntry struct {
	Command string
	Term    uint
}

type AppendEntriesMsg struct {
	args   *AppendEntriesArgs
	result *AppendEntriesResult
	done   chan struct{}
}

type AppendEntriesArgs struct {
	Term         uint
	LeaderId     string
	PrevLogIndex uint
	PrevLogTerm  uint
	Entries      []LogEntry
	LeaderCommit uint
}

type AppendEntriesResult struct {
	Term    uint
	Success bool
}

type RequestVoteMsg struct {
	args   *RequestVoteArgs
	result *RequestVoteResult
	done   chan struct{}
}

type RequestVoteArgs struct {
	Term         uint
	CandidateId  string
	LastLogIndex uint
	LastLogTerm  uint
}

type RequestVoteResult struct {
	Term        uint
	VoteGranted bool
}
