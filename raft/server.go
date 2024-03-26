package raft

import (
	"log"
	"math/rand"
	"time"
)

const (
	minServersForElection    = 3
	discoveryTimeoutDuration = 50 * time.Millisecond
)

type Server struct {
	id               string
	leader           string
	currentTerm      uint
	votedFor         string
	log              []LogEntry
	commitIndex      uint
	lastApplied      uint
	votes            uint
	servers          map[string]serverInfo
	discoveryTimeout *time.Timer
	electionTimeout  *time.Timer
	voteResultsChan  chan *RequestVoteResult
}

func NewServer(id string) *Server {
	return &Server{
		id:          id,
		leader:      "",
		currentTerm: 0,
		votedFor:    "",
		log:         make([]LogEntry, 0),
		commitIndex: 0,
		lastApplied: 0,
		votes:       0,
		servers:     make(map[string]serverInfo),
	}
}

func (server *Server) Start(gateway Gateway) {
	log.Println(server.id, "Starting Raft Server")

	server.discoveryTimeout = time.NewTimer(discoveryTimeoutDuration)
	server.electionTimeout = time.NewTimer(newElectionTimoutDuration())

	for {
		select {
		case <-server.discoveryTimeout.C:
			server.discover(gateway)
			if server.id == server.leader {
				server.sendHeartBeats(gateway)
			}
			server.discoveryTimeout.Reset(discoveryTimeoutDuration)
		case <-server.electionTimeout.C:
			electionTimoutDuration := newElectionTimoutDuration()
			server.electionTimeout.Reset(electionTimoutDuration)
			if server.id != server.leader {
				server.startElection(gateway)
			}
		case result := <-server.voteResultsChan:
			if result.VoteGranted && result.Term == server.currentTerm {
				server.votes++
				if float32(server.votes)/float32(len(server.servers)) > 0.5 {
					log.Println(server.id, "Promoted to leader")
					server.leader = server.id
				}
			}
		case msg := <-gateway.AppendEntriesMsg():
			server.handleAppendEntries(msg.args, msg.result)
			msg.done <- struct{}{}
		case msg := <-gateway.RequestVoteMsg():
			server.handleRequestVote(msg.args, msg.result)
			msg.done <- struct{}{}
		}
	}
}

func newElectionTimoutDuration() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
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

	for _, serverId := range result.Servers {
		if _, ok := server.servers[serverId]; !ok {
			server.servers[serverId] = serverInfo{
				id: serverId,
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

	server.voteResultsChan = make(chan *RequestVoteResult, len(server.servers))
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

func (server *Server) sendHeartBeats(gateway Gateway) {
	log.Println(server.id, "SendHeartBeats")

	lastLogIndex, lastLogTerm := server.lastLogIndexAndTerm()

	args := &AppendEntriesArgs{
		Term:         server.currentTerm,
		LeaderId:     server.id,
		PrevLogTerm:  lastLogTerm,
		PrevLogIndex: lastLogIndex,
		Entries:      []LogEntry{},
		LeaderCommit: server.commitIndex,
	}

	for _, info := range server.servers {
		result := &AppendEntriesResult{}
		if err := gateway.AppendEntries(info.id, args, result); err != nil {
			log.Println(err)
			continue
		}

	}
}

func (server *Server) handleAppendEntries(args *AppendEntriesArgs, result *AppendEntriesResult) {
	defer func() {
		log.Println(server.id, "HandleAppendEntries", args, result)
	}()

	if args.Term < server.currentTerm {
		result.Success = false
		return
	}

	server.electionTimeout.Reset(newElectionTimoutDuration())
	server.currentTerm = args.Term
	server.leader = args.LeaderId
	result.Success = true
}

func (server *Server) handleRequestVote(args *RequestVoteArgs, result *RequestVoteResult) {
	defer func() {
		result.Term = server.currentTerm
		log.Println(server.id, "HandleRequestVote", args, result)
	}()

	if args.Term < server.currentTerm {
		result.VoteGranted = false
		return
	} else if args.Term > server.currentTerm {
		server.currentTerm = args.Term
		server.votedFor = ""
		server.leader = ""
	}

	if server.votedFor == "" || server.votedFor == args.CandidateId {
		if args.LastLogIndex >= server.lastApplied && (server.lastApplied == 0 || args.LastLogTerm >= server.log[server.lastApplied-1].Term) {
			result.VoteGranted = true
			server.votedFor = args.CandidateId
		}
	}
}
