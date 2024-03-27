package raft

import (
	"log"
	"math/rand"
	"time"
)

const (
	minServersForElection    = 3
	heartbeatTimeoutDuration = 50 * time.Millisecond
	minElectionTimeoutMs     = 150
	maxElectionTimeoutMs     = 300
)

type Server struct {
	id                string
	leader            string
	currentTerm       uint
	votedFor          string
	log               []LogEntry
	commitIndex       uint
	lastApplied       uint
	votes             uint
	servers           map[string]serverInfo
	heartbeatTimeout  *time.Timer
	electionTimeout   *time.Timer
	voteResultsChan   chan *RequestVoteResult
	appendResultsChan chan *AppendEntriesResult
}

func NewServer(id string) *Server {
	return &Server{
		id:                id,
		leader:            "",
		currentTerm:       0,
		votedFor:          "",
		log:               make([]LogEntry, 0),
		commitIndex:       0,
		lastApplied:       0,
		votes:             0,
		servers:           make(map[string]serverInfo),
		voteResultsChan:   make(chan *RequestVoteResult),
		appendResultsChan: make(chan *AppendEntriesResult),
	}
}

func (server *Server) Start(gateway Gateway) {
	log.Println(server.id, "Starting Raft Server")

	server.heartbeatTimeout = time.NewTimer(heartbeatTimeoutDuration)
	server.electionTimeout = time.NewTimer(newElectionTimoutDuration())

	for {
		// Handle events
		select {
		case <-server.heartbeatTimeout.C:
			server.heartbeatTimeout.Reset(heartbeatTimeoutDuration)
			server.discover(gateway)
			if server.id == server.leader {
				server.sendAppendEntries(gateway)
			}
		case <-server.electionTimeout.C:
			server.electionTimeout.Reset(newElectionTimoutDuration())
			if server.id != server.leader {
				server.startElection(gateway)
			}
		case result := <-server.voteResultsChan:
			server.handleExternalTerm(result.Term)
			server.handleRequestVoteResult(result, gateway)
		case result := <-server.appendResultsChan:
			server.handleExternalTerm(result.Term)
			server.HandleAppendEntriesResult(result)
		case msg := <-gateway.AppendEntriesMsg():
			server.handleExternalTerm(msg.args.Term)
			server.handleAppendEntries(msg.args, msg.result)
			msg.done <- struct{}{}
		case msg := <-gateway.RequestVoteMsg():
			server.handleExternalTerm(msg.args.Term)
			server.handleRequestVote(msg.args, msg.result)
			msg.done <- struct{}{}
		}

		// Update State Machine
		for server.commitIndex > server.lastApplied {
			server.lastApplied++
		}
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
	// log.Println(server.id, "Discover")
	args := &DiscoverArgs{
		Id: server.id,
	}
	result := &DiscoverResult{}
	if err := gateway.Discover(args, result); err != nil {
		log.Println(err)
		return
	}

	lastLogIndex, _ := server.lastLogIndexAndTerm()
	for _, serverId := range result.Servers {
		if _, ok := server.servers[serverId]; !ok {
			server.servers[serverId] = serverInfo{
				id:         serverId,
				nextIndex:  lastLogIndex + 1,
				matchIndex: 0,
			}
		}
	}
}

func (server *Server) startElection(gateway Gateway) {
	log.Println(server.id, "Starting Election...")
	server.currentTerm++
	server.votes = 1
	server.votedFor = server.id

	if len(server.servers)+1 < minServersForElection {
		log.Println(server.id, "Not enough servers for election")
		return
	}

	lastLogIndex, lastLogTerm := server.lastLogIndexAndTerm()

	args := &RequestVoteArgs{
		Term:         server.currentTerm,
		CandidateId:  server.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	go func() {
		for _, info := range server.servers {
			go func() {
				result := &RequestVoteResult{}
				err := gateway.RequestVote(info.id, args, result)
				if err != nil {
					log.Println(err)
					return
				}
				server.voteResultsChan <- result
			}()
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

func (server *Server) sendAppendEntries(gateway Gateway) {
	log.Println(server.id, "SendAppendEntries")

	lastLogIndex, _ := server.lastLogIndexAndTerm()

	for _, info := range server.servers {
		entries := []LogEntry{}

		if info.nextIndex == 0 {
			log.Println(server.id, "Error: nextIndex is 0 for", info.id)
			continue
		}

		if lastLogIndex >= info.nextIndex {
			entries = server.log[info.nextIndex:]
		}

		var prevLogIndex uint = info.nextIndex - 1
		var prevLogTerm uint = 0
		if prevLogIndex > 0 {
			prevLogTerm = server.log[prevLogIndex-1].Term
		}

		args := &AppendEntriesArgs{
			Term:         server.currentTerm,
			LeaderId:     server.id,
			PrevLogTerm:  prevLogTerm,
			PrevLogIndex: prevLogIndex,
			Entries:      entries,
			LeaderCommit: server.commitIndex,
		}

		go func() {
			result := &AppendEntriesResult{}
			if err := gateway.AppendEntries(info.id, args, result); err != nil {
				log.Println(err)
				return
			}
			server.appendResultsChan <- result
		}()
	}
}

func (server *Server) HandleAppendEntriesResult(result *AppendEntriesResult) {
	log.Println(server.id, "HandleAppendEntriesResult", result)
	info := server.servers[result.Id]
	if result.Success {
		lastLogIndex, _ := server.lastLogIndexAndTerm()
		info.matchIndex = info.nextIndex - 1
		info.nextIndex = lastLogIndex + 1
	} else {
		info.nextIndex--
		// should send AppendEntries for this specific server
	}
}

func (server *Server) handleAppendEntries(args *AppendEntriesArgs, result *AppendEntriesResult) {
	defer func() {
		result.Id = server.id
		result.Term = server.currentTerm
		log.Println(server.id, "HandleAppendEntries", args, result)
	}()

	if args.Term == server.currentTerm && server.leader == server.id {
		log.Println(server.id, "Leader Collision!!!")
	}

	// Condition #1
	// Reply false if term < currentTerm
	if args.Term < server.currentTerm {
		result.Success = false
		return
	}

	// Condition #2
	// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if len(server.log) < int(args.PrevLogIndex) || (len(server.log) > 0 && server.log[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
		result.Success = false
		return
	}

	var newEntryIndex uint
	for relativeIndex, entry := range args.Entries {
		newEntryIndex = args.PrevLogIndex + uint(relativeIndex) + 1

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
	log.Println(server.id, "HandleRequestVoteResult", result)
	if result.VoteGranted && result.Term == server.currentTerm {
		server.votes++
		if float32(server.votes)/float32(len(server.servers)) > 0.5 {
			log.Println(server.id, "Promoted to leader")
			server.leader = server.id
			lastLogIndex, _ := server.lastLogIndexAndTerm()
			for _, info := range server.servers {
				info.nextIndex = lastLogIndex + 1
				info.matchIndex = 0
			}
			server.sendAppendEntries(gateway)
		}
	}
}

func (server *Server) handleRequestVote(args *RequestVoteArgs, result *RequestVoteResult) {
	defer func() {
		result.Term = server.currentTerm
		log.Println(server.id, "HandleRequestVote", args, result)
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
