package raft

// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
    "math/rand"
    "sync"
    "time"
)
import "labrpc"

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
    CommandValid bool
    Command      interface{}
    CommandIndex int
}

// 服务器状态/角色
const (
    FOLLOWER = iota
    CANDIDATE
    LEADER
)

const AppendEntriesTimeInterval = time.Millisecond * 75

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
    mu        sync.Mutex          // Lock to protect shared access to this peer's state
    peers     []*labrpc.ClientEnd // RPC end points of all peers
    persister *Persister          // Object to hold this peer's persisted state
    me        int                 // this peer's index into peers[]

    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.

    currentTerm int
    votedFor    map[int]bool
    // TODO 数组的长度？
    log [10]*LogEntry

    commitIndex int
    lastApplied int

    currentIndex int
    state        int
    // 当前Server选取过期时间(毫秒)
    electionTimeout         int64
    lastLeadershipHeartbeat int64
    keepLeadershipTicker    *time.Ticker
}

type LogEntry struct {
    index int
    term int
    command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

    // Your code here (2A).
    return rf.currentTerm, rf.state == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
    // Your code here (2C).
    // Example:
    // w := new(bytes.Buffer)
    // e := labgob.NewEncoder(w)
    // e.Encode(rf.xxx)
    // e.Encode(rf.yyy)
    // data := w.Bytes()
    // rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
    if data == nil || len(data) < 1 { // bootstrap without any state?
        return
    }
    // Your code here (2C).
    // Example:
    // r := bytes.NewBuffer(data)
    // d := labgob.NewDecoder(r)
    // var xxx
    // var yyy
    // if d.Decode(&xxx) != nil ||
    //    d.Decode(&yyy) != nil {
    //   error...
    // } else {
    //   rf.xxx = xxx
    //   rf.yyy = yyy
    // }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
    // Your data here (2A, 2B).
    Term         int
    CandidateId  int
    LastLogIndex int
    LastLogTerm  int
}

type AppendEntriesArgs struct {
    Term         int
    LeaderId     int
    preLogIndex  int
    prevLogTerm  int
    entries      []LogEntry
    leaderCommit int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
    // Your data here (2A).
    Term        int
    VoteGranted bool
}

type AppendEntriesReply struct {
    Term    int
    Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    // Your code here (2A, 2B).
    rf.mu.Lock()
    defer rf.mu.Unlock()
    term := rf.currentTerm

    if term >= args.Term {
        reply.VoteGranted = false
        reply.Term = term
        DPrintf("%d拒绝%d的投票请求, 当前term: %d, 请求term: %d.", rf.me, args.CandidateId, rf.currentTerm, args.Term)
        return
    }

    rf.state = FOLLOWER
    rf.currentTerm = args.Term
    reply.VoteGranted = true
    reply.Term = args.Term
    DPrintf("%d投票给%d, 当前term: %d, 请求term: %d.", rf.me, args.CandidateId, rf.currentTerm, args.Term)

}

// 维持leadership
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    reply.Term = rf.currentTerm

    if rf.currentTerm > args.Term {
        reply.Success = false
        DPrintf("%d拒绝%d的leader rpc, 请求term: %d, 当前term: %d.", rf.me, args.LeaderId, args.Term, rf.currentTerm)
        return
    }

    if rf.state == LEADER {
        rf.keepLeadershipTicker.Stop()
        DPrintf("%d停止发送leader rpc.", rf.me)
    }
    rf.lastLeadershipHeartbeat = currentMills()
    rf.state = FOLLOWER
    rf.currentTerm = args.Term
    reply.Success = true
    DPrintf("%d接受%d的领导, 请求term: %d, 当前term: %d.", rf.me, args.LeaderId, args.Term, rf.currentTerm)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
    ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
    return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
    // Your code here (2B).

    if rf.state != LEADER {
        return rf.currentIndex, rf.currentTerm, false
    }

    rf.mu.Lock()
    index := rf.currentIndex
    rf.currentIndex++
    rf.mu.Unlock()

    go startReplicate(index, command, rf)

    return index, rf.currentTerm, true
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
    // Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
    persister *Persister, applyCh chan ApplyMsg) *Raft {
    rf := &Raft{}
    rf.peers = peers
    rf.persister = persister
    rf.me = me

    // Your initialization code here (2A, 2B, 2C).

    initRaft(rf)

    startServer(peers[me])

    go startLeadershipChecker(rf)

    // initialize from state persisted before a crash
    rf.readPersist(persister.ReadRaftState())

    return rf
}

// 对raft进行一些自定义的初始化工作
func initRaft(raft *Raft) {
    raft.currentTerm = 0
    raft.state = FOLLOWER
    raft.lastLeadershipHeartbeat = currentMills()

    //lab 2B
    raft.currentIndex = 0
    raft.commitIndex = -1
    raft.lastApplied = -1
}

func startServer(peer *labrpc.ClientEnd) {
    net := labrpc.MakeNetwork()
    service := labrpc.MakeService(Raft{})
    server := labrpc.MakeServer()
    server.AddService(service)
    net.AddServer(peer, server)
}

func startLeadershipChecker(raft *Raft) {
    d := randomElectionTimeout(raft)
    DPrintf("%d初始超时时间: %d毫秒.", raft.me, raft.electionTimeout)
    timer := time.NewTimer(d)

    for {
        <-timer.C

        raft.mu.Lock()
        state := raft.state
        raft.mu.Unlock()

        switch state {
        case FOLLOWER:
            now := currentMills()
            timer.Reset(d)
            if now - raft.lastLeadershipHeartbeat >= raft.electionTimeout {
                DPrintf("Follower%d超时, 开始选举, 选举超时时间: %d毫秒.\n", raft.me, raft.electionTimeout)
                go elect(raft)
            }
            break
        case CANDIDATE:
            d = randomElectionTimeout(raft)
            timer.Reset(d)
            DPrintf("Candidate: %d选举失败，重新开始选举超时时间: %d毫秒.\n", raft.me, raft.electionTimeout)
            go elect(raft)
            break
        case LEADER:
            break
        }
    }
}

func currentMills() int64 {
    return time.Now().UnixNano() / 1e6
}

func millToNano(mill int64) int64 {
    return mill * 1e6
}

func randomElectionTimeout(raft *Raft) time.Duration {
    // 选取超时时间的范围为150-300ms
    raft.electionTimeout = int64(150 + rand.Intn(151))
    return time.Duration(millToNano(raft.electionTimeout))
}

func elect(raft *Raft) {
    switchToCandidate(raft)

    raft.votedFor = make(map[int]bool)
    raft.votedFor[raft.me] = true

    requestVotes(raft)
}

func switchToCandidate(raft *Raft) {
    raft.mu.Lock()
    raft.state = CANDIDATE
    raft.currentTerm++
    raft.mu.Unlock()
}

func requestVotes(raft *Raft) {
    for index := range raft.peers {
        if index == raft.me {
            continue
        }

        voteArgs := RequestVoteArgs{Term: raft.currentTerm, CandidateId: raft.me}
        go requestVote(raft, &voteArgs, index)
    }
}

func requestVote(raft *Raft, voteArgs *RequestVoteArgs, index int) {
    voteReply := RequestVoteReply{}
    DPrintf("%d向%d发送投票申请，当前term: %d.", raft.me, index, raft.currentTerm)
    ok := raft.sendRequestVote(index, voteArgs, &voteReply)

    // 请求发送失败，也就是得不到投票结果，raft算法应该可以自动处理此种异常
    if !ok {
        DPrintf("%d向%d发送投票请求失败, term: %d.", raft.me, index, raft.currentTerm)
        return
    }

    handleVoteReply(index, raft, voteArgs, &voteReply)
}

// 处理投票结果：
// 如果voteGranted为true，那么说明投票被接受，此时把服务器保存到votedFor中
// 如果voteGranted为false，说明存在更新的term，那么将自己设置为follower，并且term更新为返回中的值
func handleVoteReply(index int, raft *Raft, voteArgs *RequestVoteArgs, voteReply *RequestVoteReply) {
    raft.mu.Lock()
    defer raft.mu.Unlock()

    // 说明当前server已经开始了新一轮candidate选举，老的投票结果不再关心
    if voteArgs.Term < raft.currentTerm {
        DPrintf("%d丢弃%d的%d term同意票，当前term: %d.", raft.me, index, voteArgs.Term, raft.currentTerm)
        return
    }

    if voteReply.VoteGranted {
        convertToLeaderIfNecessary(raft, index)
        return
    }

    if voteReply.Term <= raft.currentTerm {
        // 说明对方也是candidate
        return
    }

    // 对方的term更高，转为follower
    raft.state = FOLLOWER
    raft.currentTerm = voteReply.Term
    go startLeadershipChecker(raft)
}

// 如果已获得多数票，转为leader
// 此函数一定要在持有锁的情况下调用
func convertToLeaderIfNecessary(raft *Raft, index int) {
    DPrintf("%d收到%d的赞成响应, term: %d.", raft.me, index, raft.currentTerm)
    if raft.state == LEADER {
        // 已经获得了多数票，不需要再做任何处理
        return
    }

    raft.votedFor[index] = true
    if !hasReceivedMajorityVote(raft) {
        return
    }

    raft.state = LEADER
    raft.votedFor = nil
    DPrintf("%d选举成功，term: %d.", raft.me, raft.currentTerm)

    go startKeepLeaderShip(raft)
}

func hasReceivedMajorityVote(raft *Raft) bool {
    total := len(raft.peers)
    received := len(raft.votedFor)

    return total / received < 2
}

func startKeepLeaderShip(raft *Raft) {
    // 应小于超时的最大时长
    ticker := time.NewTicker(AppendEntriesTimeInterval)
    raft.keepLeadershipTicker = ticker

    // 先立即执行一次
    doKeepLeaderShip(raft)

    for range ticker.C {
        doKeepLeaderShip(raft)
    }
}

func doKeepLeaderShip(raft *Raft) {
    for index := range raft.peers {
        if index == raft.me {
            continue
        }
        index := index
        go func() {
            args := AppendEntriesArgs{Term: raft.currentTerm, LeaderId: raft.me}
            reply := AppendEntriesReply{}
            DPrintf("%d向%d发送leader rpc请求, term: %d.", raft.me, index, raft.currentTerm)
            ok :=raft.sendAppendEntries(index, &args, &reply)

            if !ok {
                DPrintf("%d向%d发送leadership rpc失败, term: %d.", raft.me, index, raft.currentTerm)
                return
            }

            raft.mu.Lock()
            if !reply.Success && raft.state == LEADER {
                DPrintf("%d维持leadership失败，不再发送leadership rpc.", raft.me)
                // 没有成功，说明集群中有了更高的term，所以当前server应转为follower
                raft.currentTerm = reply.Term
                raft.state = FOLLOWER
                raft.keepLeadershipTicker.Stop()
                raft.lastLeadershipHeartbeat = currentMills()
                go startLeadershipChecker(raft)
            }
            raft.mu.Unlock()
        }()
    }
}

// 当前为leader，开始向follower发送AppendEntries消息
func startReplicate(logIndex int, command interface{}, raft *Raft) {
    entry := LogEntry{
        index:   logIndex,
        term:    raft.currentTerm,
        command: command,
    }

    raft.log[logIndex] = &entry

    for index := range raft.peers {
        if index == raft.me {
            continue
        }
        go replicateToPeer(index, raft)
    }
}

func replicateToPeer(index int, raft *Raft) {
    prevLogTerm := -1
    if index > 0 {
        prevLogTerm = raft.log[index-1].term
    }

    args := AppendEntriesArgs{
        Term:        raft.currentTerm,
        LeaderId:    raft.me,
        preLogIndex: index - 1,
        prevLogTerm: prevLogTerm,
        // TODO 之后再处理leader挂掉新leader需要覆盖follower数据的情况
        entries:      nil,
        leaderCommit: raft.commitIndex,
    }
    reply := AppendEntriesReply{}

    for {
        ok := raft.sendAppendEntries(index, &args, &reply)
        if ok {
            break
        }
    }

    // TODO 如果success为false，说明follower已经落后与leader，需要同步，稍后处理
    if reply.Success {

    }

}