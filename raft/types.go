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

type Message interface {
	Done()
}

func newMessageImpl() messageImpl {
	return messageImpl{
		done: make(chan struct{}),
	}
}

type messageImpl struct {
	done chan struct{}
}

func (m messageImpl) Done() {
	m.done <- struct{}{}
}

type AppendEntriesMsg struct {
	Message
	args   *AppendEntriesArgs
	result *AppendEntriesResult
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
	Message
	args   *RequestVoteArgs
	result *RequestVoteResult
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
	Message
	Args   *CommandArgs
	Result *CommandResult
}

type CommandArgs struct {
	Command string
}

type CommandResult struct {
	Success  bool
	Value    interface{}
	Redirect string
}
