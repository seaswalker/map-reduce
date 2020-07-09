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
    "encoding/json"
    "log"
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
    log         []*LogEntry

    commitIndex int
    lastApplied int

    // 每个follower下一个entry存放位置
    nextIndex []int
    // 每个follower最高的已经完成备份的位置
    matchIndex []int

    // 自定义属性(paper未定义的)
    // 定时发送心跳，leader专属
    heartbeatTicker         *time.Ticker
    lastLeadershipHeartbeat int64
    // 当前Server选取过期时间(毫秒)
    electionTimeout int64
    currentIndex    int
    state           int
    // 通知commit协程起来干活啦
    commitCondition *sync.Cond
    applyCh         chan ApplyMsg
}

type LogEntry struct {
    Index   int
    Term    int
    Command interface{}
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
    LeaderId    int
    PreLogIndex int
    PrevLogTerm  int
    Entries      []*LogEntry
    LeaderCommit int
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

    if !canVoteFor(rf, args) {
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

// 处理AppendEntries请求(维持leader地位、log复制)
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    reply.Term = rf.currentTerm

    if rf.currentTerm > args.Term {
        reply.Success = false
        DPrintf("%d拒绝%d的心跳, 请求term: %d, 当前term: %d.", rf.me, args.LeaderId, args.Term, rf.currentTerm)
        return
    }

    // 存在这样的情况: 当follower宕机又重新上线(term = 0)时，当前server既是follower，其term又比请求中的小
    if rf.currentTerm < args.Term {
        rf.currentTerm = args.Term
    }

    if rf.state != FOLLOWER {
        switchToFollower(rf, args.Term)
    }

    if args.Entries != nil {
        if !checkPrevLog(rf, args) {
            reply.Success = false
            return
        }
        replicateToLocal(rf, args)
    }

    // commit
    if args.LeaderCommit > rf.commitIndex {
        // 封锁后续commit请求
        DPrintf(
            "%d commitIndex: %d, leaderIndex: %d, 提交到：%d.",
            rf.me, rf.commitIndex, args.LeaderCommit, args.LeaderCommit,
            )

        rf.commitIndex = args.LeaderCommit
        signalCommit(rf)
    }

    reply.Success = true
    // 可能在switchToFollower函数中被更新过一次了...
    rf.lastLeadershipHeartbeat = currentMills()
    DPrintf("%d接受%d的领导, 请求term: %d, 当前term: %d.", rf.me, args.LeaderId, args.Term, rf.currentTerm)
}

func canVoteFor(raft *Raft, args *RequestVoteArgs) bool {
    if raft.currentTerm >= args.Term {
        return false
    }

    if raft.currentIndex == 0 {
        return true
    }

    lastLogEntry := raft.log[raft.currentIndex - 1]
    if lastLogEntry.Term > args.LastLogTerm {
        return false
    }

    if lastLogEntry.Term < args.LastLogTerm {
        return true
    }

    return raft.currentIndex - 1 <= args.LastLogIndex
}

func checkPrevLog(raft *Raft, args *AppendEntriesArgs) bool {
    if args.PreLogIndex >= raft.currentIndex {
        return false
    }

    if args.PreLogIndex == -1 {
        return true
    }

    return raft.log[args.PreLogIndex].Term == args.Term
}

func replicateToLocal(raft *Raft, args *AppendEntriesArgs) {
    writeIndex := args.PreLogIndex
    for i := 0; i < len(args.Entries); i++ {
        writeIndex++
        raft.log[writeIndex] = args.Entries[i]
    }

    diff := 0
    for i := writeIndex + 1; i < raft.currentIndex; i++ {
        // 删除冲突的entry(term不一致)
        if raft.log[i].Term != args.Term {
            raft.log[i] = nil
            diff++
        }
    }
    raft.currentIndex -= diff

    if writeIndex >= raft.currentIndex {
        raft.currentIndex = writeIndex + 1
    }
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

    index := startAgreement(command, rf)

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
    rf.applyCh = applyCh

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
    raft.log = make([]*LogEntry, 16)
    raft.commitCondition = sync.NewCond(new(sync.Mutex))
    go commitLog(raft)
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

// 当收到term更高的AppendEntries请求或向其它服务器发送AppendEntries失败时
func switchToFollower(raft *Raft, term int) {
    if raft.state == LEADER {
        raft.heartbeatTicker.Stop()
        go startLeadershipChecker(raft)
    }
    raft.state = FOLLOWER
    raft.lastLeadershipHeartbeat = currentMills()
    raft.currentTerm = term
}

func requestVotes(raft *Raft) {
    voteArgs := RequestVoteArgs{Term: raft.currentTerm, CandidateId: raft.me, LastLogIndex: -1, LastLogTerm: -1}
    if raft.currentIndex > 0 {
        lastLogEntry := raft.log[raft.currentIndex - 1]
        voteArgs.LastLogIndex = lastLogEntry.Index
        voteArgs.LastLogTerm = lastLogEntry.Term
    }

    for index := range raft.peers {
        if index == raft.me {
            continue
        }
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

    doConvertToLeader(raft)
}

func hasReceivedMajorityVote(raft *Raft) bool {
    total := len(raft.peers)
    received := len(raft.votedFor)

    return total / received < 2
}

func doConvertToLeader(raft *Raft) {
    raft.state = LEADER
    raft.votedFor = nil
    DPrintf("%d选举成功，term: %d.", raft.me, raft.currentTerm)

    peerCount := len(raft.peers)
    raft.nextIndex = make([]int, peerCount)
    raft.matchIndex = make([]int, peerCount)

    for index, _ := range raft.nextIndex {
        raft.nextIndex[index] = raft.currentIndex
    }

    for index, _ := range raft.matchIndex {
        raft.matchIndex[index] = -1
    }

    go startHeartbeat(raft)
}

func startHeartbeat(raft *Raft) {
    // 应小于超时的最大时长
    ticker := time.NewTicker(AppendEntriesTimeInterval)
    raft.heartbeatTicker = ticker

    // 先立即执行一次
    doStartHeartbeat(raft)

    for range ticker.C {
        doStartHeartbeat(raft)
    }
}

func doStartHeartbeat(raft *Raft) {
    for index := range raft.peers {
        if index == raft.me {
            continue
        }
        index := index
        go func() {
            args := AppendEntriesArgs{Term: raft.currentTerm, LeaderId: raft.me, LeaderCommit: raft.commitIndex}
            reply := AppendEntriesReply{}
            DPrintf("%d向%d发送心跳, term: %d, commitIndex: %d.", raft.me, index, raft.currentTerm, raft.commitIndex)
            ok :=raft.sendAppendEntries(index, &args, &reply)

            if !ok {
                DPrintf("%d向%d发送leadership rpc失败, term: %d.", raft.me, index, raft.currentTerm)
                return
            }

            raft.mu.Lock()
            if !reply.Success && raft.state == LEADER {
                DPrintf("%d维持leadership失败，不再发送leadership rpc.", raft.me)
                switchToFollower(raft, reply.Term)
            }
            raft.mu.Unlock()
        }()
    }
}

// 当前为leader，开始向follower发送AppendEntries消息
func startAgreement(command interface{}, raft *Raft) int {
    raft.mu.Lock()
    logIndex := raft.currentIndex
    raft.currentIndex++

    entry := LogEntry{
        Index:   logIndex,
        Term:    raft.currentTerm,
        Command: command,
    }

    raft.log[logIndex] = &entry
    raft.mu.Unlock()

    for serverId, _ := range raft.peers {
        if serverId == raft.me {
            continue
        }
        go replicateToFollower(serverId, logIndex, raft)
    }

    return logIndex
}

func replicateToFollower(serverId int, logIndex int, raft *Raft) {
    serverNextIndex := raft.nextIndex[serverId]

    var entries []*LogEntry
    if logIndex >= serverNextIndex {
        // slice: [)
        entries = raft.log[serverNextIndex:logIndex + 1]
    }

    args := AppendEntriesArgs{
        Term:         raft.currentTerm,
        LeaderId:     raft.me,
        PreLogIndex:  serverNextIndex - 1,
        PrevLogTerm:  -1,
        Entries:      entries,
        LeaderCommit: raft.commitIndex,
    }

    if serverNextIndex > 0 {
        args.PrevLogTerm = raft.log[serverNextIndex - 1].Term
    }

    reply := AppendEntriesReply{}

    for {
        ok := raft.sendAppendEntries(serverId, &args, &reply)

        // debug
        result, _ := json.Marshal(args)
        DPrintf("%d向%d发送log replicate请求, 参数: %v.", raft.me, serverId, string(result))

        if !ok {
            continue
        }

        DPrintf("Leader: %d收到%d的AppendEntries响应: term: %d, success: %v.",
            raft.me, serverId, reply.Term, reply.Success,
            )

        if reply.Success {
            commitLogIfPossible(logIndex, serverId, raft)
            break
        }

        if serverNextIndex == 0 {
            // 正常情况不可能发生
            log.Fatalf("当%d的nextIndex为0时AppendEntries返回失败.", serverId)
        }

        raft.mu.Lock()
        // 可能有多个协程同时失败(当leader在短时间内收到多个请求时)，不能让nextIndex减为负数
        if raft.nextIndex[serverId] > 0 {
            raft.nextIndex[serverId]--
        }
        raft.mu.Unlock()

        replicateToFollower(serverId, logIndex, raft)
        break
    }
}

func commitLogIfPossible(logIndex int, serverId int, raft *Raft) {
    raft.mu.Lock()
    defer raft.mu.Unlock()

    if logIndex <= raft.commitIndex {
        // 已经commit，无需再处理
        return
    }

    if logIndex > raft.matchIndex[serverId] {
        // 有可能已经被更高index的请求顺带在follower上保存了副本，那么我们不需要再修改下面两个值
        raft.matchIndex[serverId] = logIndex
        raft.nextIndex[serverId] = logIndex + 1
    }

    if raft.log[logIndex].Term != raft.currentTerm {
        // 非当前term的entry不能提交，见paper 5.4.2
        return
    }

    replicated := 0
    // 是否已达到多数？
    for id, value := range raft.matchIndex {
        if id == raft.me || value >= logIndex {
            replicated++
        }
    }

    shouldCommit := len(raft.peers) / replicated < 2

    if !shouldCommit {
        return
    }

    raft.commitIndex = logIndex
    signalCommit(raft)
    DPrintf("Leader: %d提交到: %d.", raft.me, logIndex)
}

func commitLog(raft *Raft) {
    for {
        waitCommit(raft)

        DPrintf("")

        // 有新的entry可以commit
        for index := raft.lastApplied + 1; index <= raft.commitIndex; index++ {
            DPrintf("%d发送Apply消息, index: %d.", raft.me, index)

            applyMessage := ApplyMsg{
                CommandValid: true,
                Command:      raft.log[index].Command,
                CommandIndex: index,
            }

            raft.applyCh <- applyMessage
        }

        raft.lastApplied = raft.commitIndex
    }
}

func waitCommit(raft *Raft) {
    raft.commitCondition.L.Lock()
    for ;raft.lastApplied >= raft.commitIndex; {
        raft.commitCondition.Wait()
    }
    raft.commitCondition.L.Unlock()
}

func signalCommit(raft *Raft) {
    raft.commitCondition.L.Lock()
    raft.commitCondition.Broadcast()
    raft.commitCondition.L.Unlock()
}