package raft

import (
	"errors"
	"log/slog"
	"math/rand"
	"sync"
	"time"
)

const (
	minServersForElection    = 3
	minElectionTimeoutMs     = 150
	maxElectionTimeoutMs     = 300
	heartbeatTimeoutMs       = 50
	heartbeatTimeoutDuration = heartbeatTimeoutMs * time.Millisecond
)

type ServerConfig struct {
	Id      string
	Handler CommandHandler
	Logger  *slog.Logger
}

type Server struct {
	config            ServerConfig
	leader            string
	currentTerm       uint
	votedFor          string
	log               []LogEntry
	commitIndex       uint
	lastApplied       uint
	votes             uint
	servers           map[string]*serverInfo
	pendingCommands   map[uint]CommandMsg
	heartbeatTimeout  *time.Timer
	electionTimeout   *time.Timer
	voteResultsChan   chan *RequestVoteResult
	appendResultsChan chan *AppendEntriesResult
	closeChan         chan struct{}
	state             serverState
	stateMutex        sync.Mutex
}

func NewServer(config ServerConfig) *Server {
	return &Server{
		config:            config,
		leader:            "",
		currentTerm:       0,
		votedFor:          "",
		log:               make([]LogEntry, 0),
		commitIndex:       0,
		lastApplied:       0,
		votes:             0,
		servers:           make(map[string]*serverInfo),
		voteResultsChan:   make(chan *RequestVoteResult),
		appendResultsChan: make(chan *AppendEntriesResult),
		closeChan:         make(chan struct{}),
	}
}

func (server *Server) Start(gateway Gateway) {
	server.stateMutex.Lock()
	if server.state != serverStateClosed {
		return
	}
	server.state = serverStateRunning
	server.stateMutex.Unlock()

	server.slog(slog.LevelInfo, "Starting Server")

	go func() {
		if err := gateway.Start(); err != nil {
			server.slog(slog.LevelError, "Starting gateway", "error", err.Error())
			server.Close()
		}
	}()

	server.leader = ""
	server.pendingCommands = make(map[uint]CommandMsg)
	server.heartbeatTimeout = time.NewTimer(heartbeatTimeoutDuration)
	server.electionTimeout = time.NewTimer(newElectionTimoutDuration())

	// main event loop
eventLoop:
	for {
		select {
		case msg := <-gateway.CommandMsg():
			// handle Command
			server.handleCommand(msg, gateway)
		case msg := <-gateway.AppendEntriesMsg():
			// handle AppendEntries
			server.handleExternalTerm(msg.args.Term)
			server.handleAppendEntries(msg.args, msg.result)
			msg.done <- struct{}{}
		case msg := <-gateway.RequestVoteMsg():
			// handle RequestVote
			server.handleExternalTerm(msg.args.Term)
			server.handleRequestVote(msg.args, msg.result)
			msg.done <- struct{}{}
		case result := <-server.appendResultsChan:
			// handle AppendEntries result
			server.handleExternalTerm(result.Term)
			server.handleAppendEntriesResult(result, gateway)
		case result := <-server.voteResultsChan:
			// handle RequestVote result
			server.handleExternalTerm(result.Term)
			server.handleRequestVoteResult(result, gateway)
		case <-server.heartbeatTimeout.C:
			// heartbeat timeout
			server.heartbeatTimeout.Reset(heartbeatTimeoutDuration)
			server.discover(gateway)
			if server.config.Id == server.leader {
				server.sendAppendEntriesAll(gateway)
			}
		case <-server.electionTimeout.C:
			// election timeout
			server.electionTimeout.Reset(newElectionTimoutDuration())
			if server.config.Id != server.leader {
				server.startElection(gateway)
			}
		case <-server.closeChan:
			// set state to closing
			server.stateMutex.Lock()
			server.state = serverStateClosing
			server.stateMutex.Unlock()
			break eventLoop
		}
		server.updateStateMachine()
	}

	server.slog(slog.LevelInfo, "Stopping server")
	{ // reject all pending commands
		server.slog(slog.LevelInfo, "Rejecting pending commands")
		for index, msg := range server.pendingCommands {
			server.slog(slog.LevelInfo, "Rejecting command", "index", index, "command", msg.Args.Command)
			msg.Result.Success = false
			msg.Done <- struct{}{}
		}
	}
	{ // close gateway
		server.slog(slog.LevelInfo, "Closing gateway")
		if err := gateway.Close(); err != nil {
			server.slog(slog.LevelError, "Closing gateway", "error", err.Error())
		}
	}
	{ // flush channels
		server.slog(slog.LevelInfo, "Flushing channels")
	flushLoop:
		for {
			select {
			case <-server.appendResultsChan:
			case <-server.voteResultsChan:
			case <-server.closeChan:
			default:
				break flushLoop
			}
		}
	}
	{ // stop timers
		if !server.heartbeatTimeout.Stop() {
			<-server.heartbeatTimeout.C
		}
		if !server.electionTimeout.Stop() {
			<-server.electionTimeout.C
		}
	}
	{ // set state to closed
		server.stateMutex.Lock()
		server.state = serverStateClosed
		server.stateMutex.Unlock()
	}
	server.slog(slog.LevelInfo, "Stopped server successfully")
}

func (server *Server) Close() {
	server.stateMutex.Lock()
	defer server.stateMutex.Unlock()
	if server.state == serverStateRunning {
		server.closeChan <- struct{}{}
	}
}

func (server *Server) updateStateMachine() {
	if server.leader == server.config.Id {
		for i := uint(len(server.log)); i >= server.commitIndex+1; i-- {
			if server.log[i-1].Term == server.currentTerm {
				matched := 0
				for _, info := range server.servers {
					if info.matchIndex >= i {
						matched++
					}
				}
				if float32(matched) > float32(len(server.servers))/2 {
					server.commitIndex = i
					break
				}
			}
		}
	}
	for server.commitIndex > server.lastApplied {
		server.lastApplied++
		if server.config.Handler != nil {
			server.config.Handler(server.log[server.lastApplied-1].Command)
		}
		commandMsg, ok := server.pendingCommands[server.lastApplied]
		if ok {
			commandMsg.Result.Success = true
			commandMsg.Done <- struct{}{}
			delete(server.pendingCommands, server.lastApplied)
		}
		server.slog(slog.LevelInfo, "Applied command", "index", server.lastApplied)
	}
}

func (server *Server) handleExternalTerm(term uint) {
	if term > server.currentTerm {
		server.currentTerm = term
		server.leader = ""
		server.votedFor = ""
	}
}

func newElectionTimoutDuration() time.Duration {
	return time.Duration(minElectionTimeoutMs+rand.Intn(maxElectionTimeoutMs-minElectionTimeoutMs)) * time.Millisecond
}

func (server *Server) discover(gateway Gateway) {
	args := &DiscoverArgs{
		Id: server.config.Id,
	}
	result := &DiscoverResult{}
	if err := gateway.Discover(args, result); err != nil {
		server.slog(slog.LevelError, "discover()", "error", err.Error())
		return
	}

	lastLogIndex, _ := server.lastLogIndexAndTerm()
	for _, serverId := range result.Servers {
		if _, ok := server.servers[serverId]; !ok {
			server.servers[serverId] = &serverInfo{
				id:         serverId,
				nextIndex:  lastLogIndex + 1,
				matchIndex: 0,
			}
		}
	}
}

func (server *Server) startElection(gateway Gateway) {
	defer func() {
		server.slog(slog.LevelInfo, "Starting election", "term", server.currentTerm)
	}()
	server.currentTerm++
	server.votes = 1
	server.votedFor = server.config.Id

	if len(server.servers)+1 < minServersForElection {
		server.slog(slog.LevelInfo, "Not enough servers for election")
		return
	}

	lastLogIndex, lastLogTerm := server.lastLogIndexAndTerm()

	args := &RequestVoteArgs{
		Term:         server.currentTerm,
		CandidateId:  server.config.Id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	go func() {
		for _, info := range server.servers {
			go func(id string) {
				result := &RequestVoteResult{}
				if err := gateway.RequestVote(id, args, result); err != nil {
					return
				}
				server.voteResultsChan <- result
			}(info.id)
		}
	}()
}

func (server *Server) lastLogIndexAndTerm() (uint, uint) {
	var lastLogIndex uint = uint(len(server.log))
	var lastLogTerm uint = 0
	if lastLogIndex > 0 {
		lastLogTerm = server.log[lastLogIndex-1].Term
	}
	return lastLogIndex, lastLogTerm
}

func (server *Server) sendAppendEntries(id string, gateway Gateway) error {
	info, ok := server.servers[id]
	if !ok {
		return errors.New("server does not exist")
	}

	if info.nextIndex == 0 {
		return errors.New("nextIndex is 0 for " + info.id)
	}

	entries := []LogEntry{}
	lastLogIndex, _ := server.lastLogIndexAndTerm()
	if lastLogIndex >= info.nextIndex {
		entries = server.log[info.nextIndex-1:]
	}

	var prevLogIndex uint = info.nextIndex - 1
	var prevLogTerm uint = 0
	if prevLogIndex > 0 {
		prevLogTerm = server.log[prevLogIndex-1].Term
	}

	args := &AppendEntriesArgs{
		Term:         server.currentTerm,
		LeaderId:     server.config.Id,
		PrevLogTerm:  prevLogTerm,
		PrevLogIndex: prevLogIndex,
		Entries:      entries,
		LeaderCommit: server.commitIndex,
	}

	go func() {
		result := &AppendEntriesResult{}
		if err := gateway.AppendEntries(info.id, args, result); err != nil {
			server.slog(slog.LevelError, "sendAppendEntries()", "error", err.Error())
			return
		}
		server.appendResultsChan <- result
	}()
	return nil
}

func (server *Server) sendAppendEntriesAll(gateway Gateway) {
	server.slog(slog.LevelInfo, "sendAppendEntriesAll()")
	for id := range server.servers {
		if err := server.sendAppendEntries(id, gateway); err != nil {
			server.slog(slog.LevelError, "Sending AppendEntries for "+id, "error", err.Error())
		}
	}
}

func (server *Server) handleAppendEntriesResult(result *AppendEntriesResult, gateway Gateway) {
	server.slog(slog.LevelInfo, "handleAppendEntriesResult()", "result", result)
	info, ok := server.servers[result.Id]
	if !ok {
		return
	}
	if result.Success {
		lastLogIndex, _ := server.lastLogIndexAndTerm()
		info.matchIndex = info.nextIndex - 1
		info.nextIndex = lastLogIndex + 1
	} else {
		if info.nextIndex > 1 {
			info.nextIndex--
		}
		if err := server.sendAppendEntries(info.id, gateway); err != nil {
			server.slog(slog.LevelInfo, "handleAppendEntriesResult()", "result", result, "error", err)
		}
	}
}

func (server *Server) handleAppendEntries(args *AppendEntriesArgs, result *AppendEntriesResult) {
	defer func() {
		result.Id = server.config.Id
		result.Term = server.currentTerm
		server.slog(slog.LevelInfo, "handleAppendEntries", "args", args, "result", result)
	}()

	if args.Term == server.currentTerm && server.leader == server.config.Id {
		server.slog(slog.LevelWarn, "Leader collision!")
	}

	// Condition #1
	// Reply false if term < currentTerm
	if args.Term < server.currentTerm {
		result.Success = false
		return
	}

	// Condition #2
	// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex > 0 {
		if len(server.log) < int(args.PrevLogIndex) || (len(server.log) > 0 && server.log[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
			result.Success = false
			return
		}
	}

	newEntryIndex := args.PrevLogIndex
	for _, entry := range args.Entries {
		newEntryIndex++

		// Condition #3
		// If an existing entry conflicts with a new one (same index but different terms),
		// delete the existing entry and all that follow it
		if len(server.log) >= int(newEntryIndex) && server.log[newEntryIndex-1].Term != entry.Term {
			server.log = server.log[:newEntryIndex-1]
		}

		// Condition #4
		// Append any new entries not already in the log
		if len(server.log) < int(newEntryIndex) {
			server.log = append(server.log, entry)
		}
	}

	// Condition #5
	// If leaderCommit > commitIndex,
	// set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > server.commitIndex {
		server.commitIndex = min(args.LeaderCommit, newEntryIndex)
	}

	server.leader = args.LeaderId
	result.Success = true

	if !server.electionTimeout.Stop() {
		<-server.electionTimeout.C
	}
	server.electionTimeout.Reset(newElectionTimoutDuration())
}

func (server *Server) handleRequestVoteResult(result *RequestVoteResult, gateway Gateway) {
	server.slog(slog.LevelInfo, "handleRequestVoteResult", "result", result)
	if result.VoteGranted && result.Term == server.currentTerm && server.leader != server.config.Id {
		server.votes++
		if float32(server.votes)/float32(len(server.servers)) > 0.5 {
			server.slog(slog.LevelInfo, "Promoted to leader")
			server.leader = server.config.Id
			lastLogIndex, _ := server.lastLogIndexAndTerm()
			for _, info := range server.servers {
				info.nextIndex = lastLogIndex + 1
				info.matchIndex = 0
			}
			server.sendAppendEntriesAll(gateway)
		}
	}
}

func (server *Server) handleRequestVote(args *RequestVoteArgs, result *RequestVoteResult) {
	defer func() {
		result.Term = server.currentTerm
		server.slog(slog.LevelInfo, "handleRequestVote()", "args", args, "result", result)
	}()

	// Condition #1
	// Reply false if term < currentTerm
	if args.Term < server.currentTerm {
		result.VoteGranted = false
		return
	}

	// Condition #2
	// If votedFor is null or candidateId,
	// and candidate's log is at least as up-to-date as receiver's log, grant vote
	if server.votedFor == "" || server.votedFor == args.CandidateId {
		if args.LastLogIndex >= server.lastApplied && (server.lastApplied == 0 || args.LastLogTerm >= server.log[server.lastApplied-1].Term) {
			result.VoteGranted = true
			server.votedFor = args.CandidateId
		}
	}
}

func (server *Server) handleCommand(msg CommandMsg, gateway Gateway) {
	defer func() {
		server.slog(slog.LevelInfo, "handleCommand()", "args", msg.Args, "result", msg.Result)
	}()

	if server.config.Id == server.leader {
		server.log = append(server.log, LogEntry{
			Command: msg.Args.Command,
			Term:    server.currentTerm,
		})
		newLogIndex := len(server.log)
		server.pendingCommands[uint(newLogIndex)] = msg
		server.sendAppendEntriesAll(gateway)
	} else {
		msg.Result.Success = false
		msg.Result.Redirect = server.leader
		msg.Done <- struct{}{}
	}
}

func (server *Server) slog(level slog.Level, msg string, args ...any) {
	if server.config.Logger != nil {
		args = append([]any{"id", server.config.Id}, args...)
		switch level {
		case slog.LevelInfo:
			server.config.Logger.Info(msg, args...)
		case slog.LevelError:
			server.config.Logger.Error(msg, args...)
		case slog.LevelWarn:
			server.config.Logger.Warn(msg, args...)
		}
	}
}
