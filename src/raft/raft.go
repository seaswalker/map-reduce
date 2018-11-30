package raft

//
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
    "log"
    "math/rand"
    "strconv"
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
    state       int
    // 当前Server选取过期时间(毫秒)
    electionTimeout int
    // 最后一次接收到leader心跳时间(毫秒数)
    lastLeaderTime int64
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
    Term     int
    LeaderId int
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
    reply.Term = rf.currentTerm
    reply.VoteGranted = rf.currentTerm <= args.Term
    log.Printf("收到投票请求，候选人ID: %d，term: %d，当前term: %d，当前ID: %d，回复: %s.",
        args.CandidateId, args.Term, rf.currentTerm, rf.me, strconv.FormatBool(reply.VoteGranted),
    )
}

// 维持leadership
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    reply.Term = rf.currentTerm
    reply.Success = rf.currentTerm <= args.Term
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
    index := -1
    term := -1
    isLeader := true

    // Your code here (2B).

    return index, term, isLeader
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
    startElectIfNecessary(rf)

    // initialize from state persisted before a crash
    rf.readPersist(persister.ReadRaftState())

    return rf
}

// 对raft进行一些自定义的初始化工作
func initRaft(raft *Raft) {
    raft.currentTerm = 0
    raft.state = FOLLOWER
    now := time.Now()

    raft.lastLeaderTime = now.UnixNano() / 1000
    // 选取超时时间的范围为250-350ms
    raft.electionTimeout = 250 + rand.Intn(101)
    log.Printf("选举超时时间: %d，ID: %d.", raft.electionTimeout, raft.me)
}

func startServer(peer *labrpc.ClientEnd) {
    net := labrpc.MakeNetwork()
    service := labrpc.MakeService(Raft{})
    server := labrpc.MakeServer()
    server.AddService(service)
    net.AddServer(peer, server)
}

func startElectIfNecessary(raft *Raft) {
    // 每400毫秒执行一次，此时间间隔应该比选举时间超时时间(最大350ms)略长
    d := time.Duration(time.Millisecond * 400)

    ticker := time.NewTicker(d)

    go func() {
        for range ticker.C {
            now := time.Now().UnixNano() / 1000
            if now - raft.lastLeaderTime > 350 {
                raft.mu.Lock()
                state := raft.state
                raft.mu.Unlock()

                if state == CANDIDATE {
                    continue
                }

                raft.mu.Lock()
                // 封锁后续的选举
                raft.state = CANDIDATE
                raft.currentTerm++
                raft.mu.Unlock()

                log.Printf(
                    "上一次收到leader信息时间: %d，当前时间: %d，启动选举，当前term: %d，server id: %d.\n",
                    raft.lastLeaderTime, now, raft.currentTerm, raft.me,
                )

                go elect(raft)
            }
        }
    }()
}

func elect(raft *Raft) {
    // 向所有的其它服务器广播选举请求
    for index := range raft.peers {
        if index == raft.me {
            continue
        }

        voteArgs := RequestVoteArgs{Term: raft.currentTerm, CandidateId: raft.me}
        voteReply := RequestVoteReply{}

        go func() {
            log.Printf("%d向%d发送投票申请，当前term: %d.", raft.me, index, raft.currentTerm)
            ok := raft.sendRequestVote(index, &voteArgs, &voteReply)

            if !ok {
                log.Fatalf("发送投票请求失败，server index: %d.", index)
                return
            }

            handleVoteReply(index, raft, &voteReply)
        }()
    }
}

// 处理投票结果：
// 如果voteGranted为true，那么说明投票被接受，此时把服务器保存到votedFor中
// 如果voteGranted为false，说明被拒绝，那么将自己设置为follower，并且term更新为返回中的值
func handleVoteReply(index int, raft *Raft, voteReply *RequestVoteReply) {
    raft.mu.Lock()
    defer raft.mu.Unlock()

    if voteReply.VoteGranted {
        // 非候选状态无意义
        if raft.state != CANDIDATE {
            return
        }

        if raft.votedFor == nil {
            raft.votedFor = make(map[int]bool)
        }

        raft.votedFor[index] = true

        convertToLeaderIfNecessary(raft)
    } else {
        // TODO 如果已经成了leader，那么就不能转为follower
        raft.state = FOLLOWER
        // 清空"粉丝"
        raft.votedFor = nil
        if raft.currentTerm < voteReply.Term {
            raft.currentTerm = voteReply.Term
        }
    }

}

// 如果已获得多数票，转为leader
// 此函数一定要在持有锁的情况下调用
func convertToLeaderIfNecessary(raft *Raft) {
    if !isReceiveMajorityVote(raft) {
        return
    }

    raft.state = LEADER
    raft.votedFor = nil

    startKeepLeaderShip(raft)
}

func isReceiveMajorityVote(raft *Raft) bool {
    total := len(raft.peers)
    received := len(raft.votedFor)

    return total / received < 2
}

func startKeepLeaderShip(raft *Raft) {
    // 应小于超时的最大时长
    d := time.Duration(time.Millisecond * 200)

    ticker := time.NewTicker(d)

    go func() {
        for range ticker.C {
            doKeepLeaderShip(raft)
            log.Printf("Start to keep leadership.")
        }
    }()
 }

func doKeepLeaderShip(raft *Raft) {
    for index := range raft.peers {
        if index == raft.me {
            continue
        }

        go func() {
            args := AppendEntriesArgs{Term: raft.currentTerm, LeaderId: raft.me}
            reply := AppendEntriesReply{}
            raft.sendAppendEntries(index, &args, &reply)

            if !reply.Success {
                // ops, lose leadership...
                raft.mu.Lock()
                defer raft.mu.Unlock()
                raft.state = FOLLOWER
                raft.currentTerm = reply.Term
            }
        }()
    }
}