package raft

type CommandHandler func(command string)

type CommandMiddleware func(command string) (interface{}, bool)

type serverState uint

const (
	serverStateClosed = iota
	serverStateRunning
	serverStateClosing
)

type serverInfo struct {
	id         string
	nextIndex  uint
	matchIndex uint
}

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
	Id      string
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

type CommandMsg struct {
	Args   *CommandArgs
	Result *CommandResult
	Done   chan struct{}
}

type CommandArgs struct {
	Command string
}

type CommandResult struct {
	Success  bool
	Value    interface{}
	Redirect string
}
