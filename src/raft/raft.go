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
	"bytes"
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

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
	KILLED
)

var idStateMap = map[int]string{
	FOLLOWER:  "Follower",
	CANDIDATE: "Candidate",
	LEADER:    "Leader",
	KILLED:    "killed",
}

const logEntryArraySize = 102400

// 心跳发送间隔
const appendEntriesTimeInterval = time.Millisecond * 75
const maxQueuedEntryCount = 50

// 毫秒
const queuedEntryExpireTime = 20

// LOSELEADERSHIPAPPLYMESSAGE 表示当Raft.applyChan的监听者检测到此消息就说明对应的raft节点已不再是leader
var LOSELEADERSHIPAPPLYMESSAGE = ApplyMsg{
	CommandValid: false,
	Command:      "LOSE_LEADERSHIP",
}

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

	// 持久化变量区
	currentTerm int
	state       int
	log         []*LogEntry
	// 持久化区结束

	commitIndex int
	lastApplied int

	// 每个follower下一个entry存放位置
	nextIndex []int
	// 每个follower最高的已经完成备份的位置
	matchIndex []int

	// 自定义属性(paper未定义的)
	voteGranted map[int]bool
	// 定时发送心跳，leader专属
	heartbeatTicker         *time.Ticker
	heartbeatChan           chan bool
	lastLeadershipHeartbeat int64
	// 收到客户端请求时，缓存一定数量再向follower发起replicate请求
	queuedEntryCount int
	// 上一次收到客户端请求并进行缓存的时间(毫秒)
	lastQueueEntryTime int64
	// 定期检查排队的客户端请求是否已过期
	queuedEntryExpireCheckTicker *time.Ticker
	queuedEntryExpireCheckChan   chan bool

	// 当前Server选取过期时间(毫秒)
	electionTimeout int64
	// 单调递增(即使snapshot存在)，在64位平台是int时64位，假设保持每秒5000次请求，足够使用5800万年
	currentIndex int
	// 通知commit协程起来干活啦
	applyCondition           *sync.Cond
	applyCh                  chan ApplyMsg
	needNotifyLoseLeaderShip bool
	snapshot                 *stateSnapshot
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

type stateSnapshot struct {
	LastIncludedIndex int
	LastIncludedTerm  int
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.state == LEADER
}

// CreateSnapshot 由KVServer调用，生成快照直到index(包含)
func (rf *Raft) CreateSnapshot(kvServerDataStore []byte, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.snapshot.LastIncludedTerm = rf.log[getRealLogIndex(rf, index)].Term
	rf.log = rf.log[getRealLogIndex(rf, index+1):]
	rf.snapshot.LastIncludedIndex = index
	stateData := encodeCurrentState(rf)

	rf.persister.SaveStateAndSnapshot(stateData, kvServerDataStore)

	DPrintf("%d收到kv server快照生成请求, 截止index(包含): %d.", rf.me, index)
}

// 当作为follower收到leader的InstallSnapshot请求时调用
func (rf *Raft) createSnapshotFromLeader(kvServerDataStore []byte, index int, lastIncludeTerm int) {
	rf.snapshot.LastIncludedTerm = lastIncludeTerm
	rf.snapshot.LastIncludedIndex = index
	if rf.currentIndex <= index {
		rf.currentIndex = index + 1
		rf.log = make([]*LogEntry, logEntryArraySize)
	} else {
		rf.log = rf.log[getRealLogIndex(rf, index+1):]
	}
	stateData := encodeCurrentState(rf)

	rf.persister.SaveStateAndSnapshot(stateData, kvServerDataStore)

	rf.commitIndex = index
	rf.lastApplied = index
}

func getRealLogIndex(raft *Raft, index int) int {
	if index <= raft.snapshot.LastIncludedIndex {
		return index
	}
	return index - raft.snapshot.LastIncludedIndex - 1
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	data := encodeCurrentState(rf)
	rf.persister.SaveRaftState(data)
}

func encodeCurrentState(rf *Raft) []byte {
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	err := encoder.Encode(rf.currentTerm)
	if err != nil {
		panic(err)
	}

	err = encoder.Encode(rf.state)
	if err != nil {
		panic(err)
	}

	logArray := rf.log[0:getRealLogIndex(rf, rf.currentIndex)]
	err = encoder.Encode(logArray)
	if err != nil {
		panic(err)
	}

	err = encoder.Encode(rf.snapshot)
	if err != nil {
		panic(err)
	}

	return writer.Bytes()
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	reader := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(reader)

	var currentTerm int
	err := decoder.Decode(&currentTerm)
	if err != nil {
		panic(err)
	}

	var state int
	err = decoder.Decode(&state)
	if err != nil {
		panic(err)
	}

	var logEntries []*LogEntry
	err = decoder.Decode(&logEntries)
	if err != nil {
		panic(err)
	}

	var snapshot *stateSnapshot = &stateSnapshot{}
	err = decoder.Decode(snapshot)
	if err != nil {
		panic(err)
	}

	rf.snapshot = snapshot
	rf.currentTerm = currentTerm
	rf.state = state
	rf.currentIndex = len(logEntries) + rf.snapshot.LastIncludedIndex + 1
	// 还是保持数组原有的长度
	array := make([]*LogEntry, logEntryArraySize)
	copy(array, logEntries)
	rf.log = array
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
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
	// debug
	TraceID string
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
	// 告诉leader下次请求时发送的log数组的最低index是NextIndex
	NextIndex int
}

// InstallSnapshot leader将自己的snapshot发送给follower
type InstallSnapshot struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// InstallSnapshotReply RPC的响应
type InstallSnapshotReply struct {
	// follower的term
	Term int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = true
	reply.Term = rf.currentTerm

	if rf.currentTerm >= args.Term {
		reply.VoteGranted = false
		DPrintf(
			"%d拒绝%d的投票请求: 当前term: %d >= 请求term: %d.",
			rf.me, args.CandidateId, rf.currentTerm, args.Term,
		)
		return
	}

	// 这里虽然转为follower状态，但是不能更新心跳接收时间，否则会抑制当前server选举发起，或许在某种意义上说，
	// 只有要更新心跳时间时才算认可了对方是leader
	switchToFollower(rf, args.Term)
	if reply.Term != args.Term {
		rf.persist()
	}

	if canVote, reason := canVoteFor(rf, args); !canVote {
		reply.VoteGranted = false
		DPrintf("%d拒绝%d的投票请求: %v.", rf.me, args.CandidateId, reason)
		return
	}

	rf.lastLeadershipHeartbeat = currentMills()
	DPrintf("%d投票给%d, term: %d.", rf.me, args.CandidateId, args.Term)
}

// AppendEntries 用于维持leader地位、log复制
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args == nil || reply == nil {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if rf.currentTerm > args.Term {
		reply.Success = false
		DPrintf(
			"%d拒绝%d的AppendEntries请求, traceId: %v, 原因: %v.",
			rf.me, args.LeaderID, args.TraceID,
			fmt.Sprintf("当前term: %d > 请求term: %d", rf.currentTerm, args.Term),
		)
		return
	}

	// 存在这样的情况: 当follower宕机又重新上线(term = 0)时，当前server既是follower，其term又比请求中的小
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
	}

	acceptLeadership(rf, args.Term)

	if success, nextIndex, errorMessage := checkPrevLog(rf, args); !success {
		reply.Success = false
		reply.NextIndex = nextIndex
		DPrintf("%d拒绝%d的AppendEntries请求, nextIndex: %d, traceId: %v, 原因: %v.",
			rf.me, args.LeaderID, nextIndex, args.TraceID, errorMessage,
		)
		return
	}

	if args.Entries != nil && args.PrevLogIndex >= rf.snapshot.LastIncludedIndex {
		replicateToLocal(rf, args)
		rf.persist()
	}

	// commit
	if args.LeaderCommit > rf.commitIndex {
		// 注意：leaderCommit信息可能早于log entry到达
		actualCommitIndex := min(args.LeaderCommit, rf.currentIndex-1)
		if actualCommitIndex > rf.commitIndex {
			DPrintf(
				"%d commitIndex: %d, leaderIndex: %d, 提交到：%d, traceId: %v.",
				rf.me, rf.commitIndex, args.LeaderCommit, actualCommitIndex, args.TraceID,
			)
			rf.commitIndex = actualCommitIndex
			signalApply(rf)
		}
	}

	reply.Success = true
	DPrintf(
		"%d接受%d的AppendEntries请求, traceId: %v, leader term: %d, 当前term: %d.",
		rf.me, args.LeaderID, args.TraceID, args.Term, rf.currentTerm,
	)
}

// InstallSnapshot 处理InstallSnapshot RPC请求
func (rf *Raft) InstallSnapshot(args *InstallSnapshot, reply *InstallSnapshotReply) {
	if args == nil || reply == nil {
		return
	}

	if rf.currentTerm > args.Term {
		DPrintf("%d拒绝%d的InstallSnapshot请求, 当前term: %d > 请求term: %d.", rf.me, args.LeaderID, rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		return
	}

	if rf.snapshot.LastIncludedIndex >= args.LastIncludedIndex {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	acceptLeadership(rf, args.Term)

	rf.createSnapshotFromLeader(args.Data, args.LastIncludedIndex, args.LastIncludedTerm)

	// 将leader发过来的数据覆盖到此follower对应的KVServer
	var snapshotMessage = &ApplyMsg{
		CommandValid: false,
		Command:      args.Data,
	}

	rf.applyCh <- *snapshotMessage

	DPrintf(
		"%d接受leader: %d的install snapshot请求, leader lastIncludedIndex: %d, leader lastIncludedTerm: %d.",
		rf.me, args.LeaderID, args.LastIncludedIndex, args.LastIncludedTerm,
	)
}

func canVoteFor(raft *Raft, args *RequestVoteArgs) (canVote bool, reason string) {
	if raft.currentIndex == 0 {
		return true, ""
	}

	if raft.currentIndex-1 > args.LastLogIndex {
		return false, fmt.Sprintf(
			"lastLogIndex: %d > args.lastLogIndex: %d", raft.currentIndex-1, args.LastLogIndex)
	}

	realLastLogEntryIndex := getRealLogIndex(raft, raft.currentIndex-1)
	var lastLogEntryTerm int
	if realLastLogEntryIndex < 0 {
		lastLogEntryTerm = raft.snapshot.LastIncludedTerm
	} else {
		lastLogEntryTerm = raft.log[realLastLogEntryIndex].Term
	}

	if lastLogEntryTerm > args.LastLogTerm {
		return false, fmt.Sprintf(
			"lastLogEntry.Term: %d > args.LastLogTerm: %d", lastLogEntryTerm, args.LastLogTerm)
	}

	return true, ""
}

func checkPrevLog(raft *Raft, args *AppendEntriesArgs) (success bool, nextIndex int, reason string) {
	if args.PrevLogIndex <= raft.snapshot.LastIncludedIndex {
		return true, -1, ""
	}

	if args.PrevLogIndex >= raft.currentIndex {
		// leader比当前follower日志更多，告诉leader当前follower最新日志的index，下次leader在此index开始
		return false, raft.currentIndex, fmt.Sprintf("PrelogIndex: %d >= currentIndex: %d", args.PrevLogIndex, raft.currentIndex)
	}

	// 到这里args.PrevLogIndex必定大于raft.snap.lastIncludeIndex，原因:
	// 如果相等，那么说明必定比较的是已经commit的log，所以肯定是相等的(只有commit了的log才能被创建snapshot)
	// 根据上面的逻辑，args.PrevLogIndex等于lastIncludeIndex时就像leader返回了true, leader自然也不会再减小nextIndex并重试
	entry := raft.log[getRealLogIndex(raft, args.PrevLogIndex)]

	if entry.Term != args.PrevLogTerm {
		// 找到非当前term的最新index
		index := args.PrevLogIndex - 1
		for ; index > raft.snapshot.LastIncludedIndex && raft.log[getRealLogIndex(raft, index)].Term == entry.Term; index-- {
		}
		return false, index + 1, fmt.Sprintf("PreLogTerm: %d != leaderPrevLogTerm: %d", entry.Term, args.PrevLogTerm)
	}

	return true, -1, ""
}

func replicateToLocal(raft *Raft, args *AppendEntriesArgs) {
	writeIndex := args.PrevLogIndex
	for i := 0; i < len(args.Entries); i++ {
		writeIndex++
		raft.log[getRealLogIndex(raft, writeIndex)] = args.Entries[i]
	}

	diff := 0
	for i := writeIndex + 1; i < raft.currentIndex; i++ {
		// 删除冲突的entry(term不一致)
		if raft.log[getRealLogIndex(raft, i)].Term != args.Term {
			raft.log[realIndex] = nil
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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshot, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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

	rf.mu.Lock()
	if rf.state != LEADER {
		rf.mu.Unlock()
		return rf.currentIndex, rf.currentTerm, false
	}

	savedIndex := saveLogEntry(command, rf)
	rf.persist()

	shouldStartAgreement := false
	logIndex := 0
	rf.queuedEntryCount++
	rf.lastQueueEntryTime = currentMills()

	if rf.queuedEntryCount >= maxQueuedEntryCount {
		shouldStartAgreement = true
		logIndex = rf.currentIndex - 1
		DPrintf("客户端请求: %v导致%d缓存的请求数: %d已达上限, 立即开始replicate.", command, rf.me, rf.queuedEntryCount)
		rf.queuedEntryCount = 0
	} else {
		DPrintf("客户端请求: %v被%d缓存, 目前缓存数: %d.", command, rf.me, rf.queuedEntryCount)
	}
	rf.mu.Unlock()

	if shouldStartAgreement {
		startAgreement(rf, logIndex)
	}

	return savedIndex, rf.currentTerm, true
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	// 如果不把这些协程关闭，使用go test -count=20测试时可能会失败，而且会跑满CPU，推测go test -count多次测试始终是一个进程，所以之前
	// 创建的协程始终在运行，Kill方法会被config.cleanup调用
	if rf.heartbeatTicker != nil {
		rf.heartbeatTicker.Stop()
		rf.heartbeatChan <- true

		rf.queuedEntryExpireCheckTicker.Stop()
		rf.queuedEntryExpireCheckChan <- true
	}

	rf.state = KILLED

	signalApply(rf)
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
	raft := &Raft{}
	raft.peers = peers
	raft.persister = persister
	raft.me = me
	raft.applyCh = applyCh

	setCommonStates(raft)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	raft.readPersist(persister.ReadRaftState())

	if raft.currentTerm > 0 {
		// 说明存在持久化的状态并且读取成功
		DPrintf(
			"%d读取到持久化状态, currentTerm: %d, state: %s, log数组日志数: %d.",
			me, raft.currentTerm, idStateMap[raft.state], raft.currentIndex,
		)
		recoverFromPersistedState(raft)
	} else {
		asABrandNewFollower(raft)
	}

	registerEncoder(raft)
	startLogCommitter(raft)
	startServer(peers[me])

	return raft
}

func setCommonStates(raft *Raft) {
	raft.currentIndex = 0
	raft.commitIndex = -1
	raft.lastApplied = -1
	raft.snapshot = &stateSnapshot{LastIncludedIndex: -1, LastIncludedTerm: -1}
}

func registerEncoder(raft *Raft) {
	labgob.Register(&stateSnapshot{})
	labgob.Register(&InstallSnapshot{})
	labgob.Register(&InstallSnapshotReply{})
}

func recoverFromPersistedState(raft *Raft) {
	raft.commitIndex = raft.snapshot.LastIncludedIndex
	raft.lastApplied = raft.commitIndex

	switch raft.state {
	case FOLLOWER:
		recoverAsFollower(raft)
		break
	case CANDIDATE:
		recoverAsCandidate(raft)
		break
	case LEADER:
		recoverAsLeader(raft)
		break
	case KILLED:
		log.Fatalf("不能从KILLED状态恢复!")
	}
}

func recoverAsLeader(raft *Raft) {
	doSwitchToLeader(raft)
}

func recoverAsCandidate(raft *Raft) {
	// paper并没有说要持久化投票记录信息，所以恢复为follower
	recoverAsFollower(raft)
}

func recoverAsFollower(raft *Raft) {
	switchToFollower(raft, raft.currentTerm)
	raft.lastLeadershipHeartbeat = currentMills()
	go startLeadershipChecker(raft)
}

func asABrandNewFollower(raft *Raft) {
	raft.log = make([]*LogEntry, logEntryArraySize)
	raft.currentTerm = 0
	recoverAsFollower(raft)
}

func startLogCommitter(raft *Raft) {
	raft.applyCondition = sync.NewCond(new(sync.Mutex))
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

		state := raft.state

		switch state {
		case FOLLOWER:
			now := currentMills()
			timer.Reset(d)
			if now-raft.lastLeadershipHeartbeat >= raft.electionTimeout {
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
		case KILLED:
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

	raft.voteGranted = make(map[int]bool)
	raft.voteGranted[raft.me] = true

	requestVotes(raft)
}

func switchToCandidate(raft *Raft) {
	raft.mu.Lock()
	raft.state = CANDIDATE
	raft.currentTerm++
	raft.persist()
	raft.mu.Unlock()
}

// 当收到term更高的AppendEntries请求或向其它服务器发送AppendEntries失败时
func switchToFollower(raft *Raft, term int) {
	if raft.state == LEADER {
		raft.heartbeatTicker.Stop()
		raft.heartbeatChan <- true

		raft.queuedEntryExpireCheckTicker.Stop()
		raft.queuedEntryExpireCheckChan <- true

		raft.needNotifyLoseLeaderShip = true
		signalApply(raft)

		go startLeadershipChecker(raft)
		DPrintf("%d失去leadership, 对方term: %d.", raft.me, term)
	}

	raft.state = FOLLOWER
	raft.currentTerm = term
}

// 表示接受对方在term的领导，此方法区别于switchToFollower便是要设置最后收到心跳的时间
func acceptLeadership(raft *Raft, term int) {
	oldState := raft.state
	oldTerm := raft.currentTerm

	switchToFollower(raft, term)
	raft.lastLeadershipHeartbeat = currentMills()

	if oldState != FOLLOWER || oldTerm != term {
		DPrintf(
			"%d的AcceptLeadership触发持久化, 老状态: %s, 新状态: Follower, 老term: %d, 新term: %d.",
			raft.me, idStateMap[oldState], oldTerm, term,
		)
		raft.persist()
	}
}

func requestVotes(raft *Raft) {
	voteArgs := RequestVoteArgs{Term: raft.currentTerm, CandidateId: raft.me, LastLogIndex: -1, LastLogTerm: -1}
	if raft.currentIndex > 0 {
		lastLogEntry := raft.log[getRealLogIndex(raft, raft.currentIndex-1)]
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
	if voteArgs.Term != raft.currentTerm {
		DPrintf("%d的term已经改变, 丢弃%d的%d term投票结果，当前term: %d.",
			raft.me, index, voteArgs.Term, raft.currentTerm,
		)
		return
	}

	if voteReply.VoteGranted && raft.state == CANDIDATE {
		switchToLeaderIfNecessary(raft, index)
		return
	}

	if voteReply.Term > raft.currentTerm {
		// 可能从leader、candidate转成follower
		acceptLeadership(raft, voteReply.Term)
	}
}

// 如果已获得多数票，转为leader
// 此函数一定要在持有锁的情况下调用
func switchToLeaderIfNecessary(raft *Raft, index int) {
	DPrintf("%d收到%d的赞成响应, term: %d.", raft.me, index, raft.currentTerm)
	raft.voteGranted[index] = true
	if !hasReceivedMajorityVote(raft) {
		return
	}

	DPrintf("%d选举成功，term: %d, 触发状态持久化.", raft.me, raft.currentTerm)
	doSwitchToLeader(raft)
	raft.persist()
}

func hasReceivedMajorityVote(raft *Raft) bool {
	total := len(raft.peers)
	received := len(raft.voteGranted)

	return total/received < 2
}

func doSwitchToLeader(raft *Raft) {
	raft.state = LEADER
	raft.voteGranted = nil

	peerCount := len(raft.peers)
	raft.nextIndex = make([]int, peerCount)
	raft.matchIndex = make([]int, peerCount)

	for index := range raft.nextIndex {
		raft.nextIndex[index] = raft.currentIndex
	}

	for index := range raft.matchIndex {
		raft.matchIndex[index] = -1
	}

	// 必须在持有锁的情况下初始化ticker，不能放在下面的协程里面。考虑如果不这样做：
	// leader选举成功，执行到这里释放锁，此时集群中投否决票(term更高)的响应到来，得到锁，去执行switchFollower方法，
	// 但由于ticker尚未初始化，会导致对ticker stop操作异常(nil)
	raft.heartbeatTicker = time.NewTicker(appendEntriesTimeInterval)
	raft.heartbeatChan = make(chan bool, 1)

	raft.queuedEntryCount = 0
	raft.queuedEntryExpireCheckTicker = time.NewTicker(queuedEntryExpireTime)
	raft.queuedEntryExpireCheckChan = make(chan bool, 1)

	go heartbeat(raft)
	go checkIfQueuedEntryExpired(raft)
}

func heartbeat(raft *Raft) {
	// 先立即执行一次
	doHeartbeat(raft)

	for {
		select {
		case _ = <-raft.heartbeatTicker.C:
			if raft.state != LEADER {
				break
			}
			doHeartbeat(raft)
		case _ = <-raft.heartbeatChan:
			break
		}
	}
}

func doHeartbeat(raft *Raft) {
	for index := range raft.peers {
		if index == raft.me {
			continue
		}

		serverID := index

		go func() {
			raft.mu.Lock()
			prevLogIndex := raft.currentIndex - 1
			traceID := randstring(10)
			prevLogTerm := -1
			if prevLogIndex >= 0 {
				prevLogTerm = raft.log[getRealLogIndex(raft, prevLogIndex)].Term
			}
			raft.mu.Unlock()

			args := AppendEntriesArgs{
				Term:         raft.currentTerm,
				LeaderID:     raft.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      nil,
				LeaderCommit: raft.commitIndex,
				TraceID:      traceID,
			}

			reply := AppendEntriesReply{}

			DPrintf("%d向%d发送心跳: %#v.", raft.me, serverID, args)

			ok := raft.sendAppendEntries(serverID, &args, &reply)

			if !ok {
				return
			}

			if reply.Success {
				return
			}

			if reply.Term > raft.currentTerm {
				raft.mu.Lock()
				DPrintf("%d维持leadership失败，不再发送心跳, traceId: %v.", raft.me, args.TraceID)
				acceptLeadership(raft, reply.Term)
				raft.mu.Unlock()
				return
			}

			if raft.state != LEADER {
				return
			}
			// follower和leader的日志不匹配，开始entry弥合
			decreaseNextIndexAndRetry(raft, serverID, reply.NextIndex, traceID, prevLogIndex)
		}()
	}
}

func saveLogEntry(command interface{}, raft *Raft) int {
	logIndex := raft.currentIndex
	raft.currentIndex++

	entry := LogEntry{
		Index:   logIndex,
		Term:    raft.currentTerm,
		Command: command,
	}

	raft.log[getRealLogIndex(raft, logIndex)] = &entry
	return logIndex
}

// 当前为leader，开始向follower发送AppendEntries消息
func startAgreement(raft *Raft, logIndex int) int {
	for serverID := range raft.peers {
		if serverID == raft.me {
			continue
		}
		traceID := strconv.Itoa(logIndex) + "-" + randstring(10)
		go replicateToFollower(serverID, logIndex-1, logIndex, raft, traceID)
	}

	return logIndex
}

// AppendEntries请求的PreLogIndex和PrevLogTerm取值有两种情况：
// 1. 当向follower发送心跳或者接到客户端请求第一次向follower发送AppendEntries时，这两个值应从leader的最新一个log entry取
// 2. 如果1中的请求被follower拒绝，那么再从raft.log[raft.nextId[followerId] - 1]中取(递减一)，这便是寻找leader和follower都同意
//    的log entry的过程
// 如果不按照上述逻辑，是无法通过TestRejoin2B单元测试的
func replicateToFollower(serverID int, lowBound int, logIndex int, raft *Raft, traceID string) {
	if lowBound < raft.snapshot.LastIncludedIndex {
		if !sendInstallSnapshotRPC(raft, serverID, traceID) {
			return
		}
		lowBound = raft.snapshot.LastIncludedIndex
	}

	var entries []*LogEntry
	if logIndex >= lowBound {
		// slice: [)
		entries = raft.log[getRealLogIndex(raft, lowBound+1):getRealLogIndex(raft, logIndex+1)]
	}

	args := AppendEntriesArgs{
		Term:         raft.currentTerm,
		LeaderID:     raft.me,
		PrevLogIndex: lowBound,
		PrevLogTerm:  -1,
		Entries:      entries,
		LeaderCommit: raft.commitIndex,
		TraceID:      traceID,
	}

	if lowBound > raft.snapshot.LastIncludedIndex {
		args.PrevLogTerm = raft.log[getRealLogIndex(raft, lowBound)].Term
	} else if lowBound == raft.snapshot.LastIncludedIndex {
		args.PrevLogTerm = raft.snapshot.LastIncludedTerm
	}
	reply := AppendEntriesReply{}

	// 考虑这样的情形，leader被网络阻断，期间集群选举除了新的leader，之后网络恢复，在与集群其它server完成交互(发现更高的term)之前，
	// 收到了client写请求，随后向其它server并行发起replicate请求，其中一个请求先返回，导致当前leader转为follower，并将term更新为最新，
	// 但此时如果有其它replicate请求现在才开始，就会带着最新的term去请求，这会导致覆盖掉其它server已经commit的log，所以这里需要检查state
	for raft.state == LEADER {

		ok := raft.sendAppendEntries(serverID, &args, &reply)

		if !ok {
			// 网络问题不可达，无限重试
			continue
		}

		// debug
		if isDebugEnabled() {
			var firstLogEntry *LogEntry
			var lastLogEntry *LogEntry
			if args.Entries != nil && len(args.Entries) > 0 {
				firstLogEntry = args.Entries[0]
				lastLogEntry = args.Entries[len(args.Entries)-1]
			}
			DPrintf(
				"%d向%d发送log replicate请求成功, 响应: %v, term: %d, prevLogIndex: %d, prevLogTerm: %d, firstLogEntry: %#v, lastLogEntry: %#v, traceId: %v.",
				raft.me, serverID, reply.Success, args.Term, args.PrevLogIndex, args.PrevLogTerm, firstLogEntry, lastLogEntry, args.TraceID,
			)
		}

		if reply.Success {
			commitLogIfPossible(logIndex, serverID, raft)
			break
		}

		// 考虑这样的情形：follower网络不可用然后恢复，这时此server的term会高于当前leader(因为触发选举变为candidate)，
		// 这种情况下变为follower，等待下一轮重新选举
		if reply.Term > raft.currentTerm {
			DPrintf("%d的term: %d小于follower%d的term: %d，转为follower, traceId: %v.",
				raft.me, raft.currentTerm, serverID, reply.Term, traceID,
			)
			raft.mu.Lock()
			acceptLeadership(raft, reply.Term)
			raft.mu.Unlock()
			break
		}

		if args.Term != raft.currentTerm {
			// term已经变了，不要再重试了
			DPrintf("Term已经改变, 退出重试, traceId: %v.", traceID)
			break
		}

		// 递归
		decreaseNextIndexAndRetry(raft, serverID, reply.NextIndex, traceID, logIndex)
		break
	}
}

// 如果请求成功，返回true，否则表示对方的term更高，当前leader已转为follower
func sendInstallSnapshotRPC(raft *Raft, serverID int, traceID string) bool {
	request := &InstallSnapshot{
		Term:              raft.currentTerm,
		LeaderID:          raft.me,
		LastIncludedIndex: raft.snapshot.LastIncludedIndex,
		LastIncludedTerm:  raft.snapshot.LastIncludedTerm,
		Data:              raft.persister.ReadSnapshot(),
	}

	result := true
	reply := &InstallSnapshotReply{}
	for {
		sendResult := raft.sendInstallSnapshot(serverID, request, reply)
		if !sendResult {
			continue
		}

		if reply.Term > request.Term {
			DPrintf(
				"%d的term: %d小于follower%d的term: %d，转为follower, traceId: %v.",
				raft.me, raft.currentTerm, serverID, reply.Term, traceID,
			)
			raft.mu.Lock()
			acceptLeadership(raft, reply.Term)
			raft.mu.Unlock()
			result = false
		}

		break
	}

	return result
}

func commitLogIfPossible(logIndex int, serverID int, raft *Raft) {
	raft.mu.Lock()
	defer raft.mu.Unlock()

	if logIndex > raft.matchIndex[serverID] {
		// 有可能已经被更高index的请求顺带在follower上保存了副本，那么我们不需要再修改下面两个值
		raft.matchIndex[serverID] = logIndex
		raft.nextIndex[serverID] = logIndex + 1
	}

	if logIndex <= raft.commitIndex {
		// 已经commit，无需再处理
		return
	}

	if raft.log[getRealLogIndex(raft, logIndex)].Term != raft.currentTerm {
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

	shouldCommit := len(raft.peers)/replicated < 2

	if !shouldCommit {
		return
	}

	raft.commitIndex = logIndex
	signalApply(raft)
	DPrintf("Leader: %d提交到: %d.", raft.me, logIndex)
}

func commitLog(raft *Raft) {
	for {
		waitApply(raft)

		if raft.state == KILLED {
			break
		}

		// 有新的entry可以commit
		for index := raft.lastApplied + 1; index <= raft.commitIndex; index++ {
			DPrintf("%d发送Apply消息, index: %d.", raft.me, index)

			realIndex := getRealLogIndex(raft, index)
			applyMessage := ApplyMsg{
				CommandValid: true,
				Command:      raft.log[realIndex].Command,
				CommandIndex: index,
			}

			raft.applyCh <- applyMessage
		}

		if raft.needNotifyLoseLeaderShip {
			raft.applyCh <- LOSELEADERSHIPAPPLYMESSAGE
			raft.needNotifyLoseLeaderShip = false
			DPrintf("%d发送lose leadership消息成功.", raft.me)
		}

		raft.lastApplied = raft.commitIndex
	}
}

func checkIfQueuedEntryExpired(raft *Raft) {
	for {
		select {
		case _ = <-raft.queuedEntryExpireCheckTicker.C:
			raft.mu.Lock()
			if raft.state != LEADER {
				raft.mu.Unlock()
				break
			}

			now := currentMills()
			if now-raft.lastQueueEntryTime >= queuedEntryExpireTime && raft.queuedEntryCount > 0 {
				raft.lastQueueEntryTime = now
				logIndex := raft.currentIndex - 1
				DPrintf("缓存的%d个客户端请求已过期, 开始replicate.", raft.queuedEntryCount)
				raft.queuedEntryCount = 0
				startAgreement(raft, logIndex)
			}
			raft.mu.Unlock()
		case _ = <-raft.queuedEntryExpireCheckChan:
			break
		}
	}
}

func waitApply(raft *Raft) {
	raft.applyCondition.L.Lock()
	for raft.lastApplied >= raft.commitIndex && !raft.needNotifyLoseLeaderShip {
		raft.applyCondition.Wait()
	}
	raft.applyCondition.L.Unlock()
}

func signalApply(raft *Raft) {
	raft.applyCondition.L.Lock()
	raft.applyCondition.Broadcast()
	raft.applyCondition.L.Unlock()
}

func decreaseNextIndexAndRetry(raft *Raft, serverID int, nextIndex int, traceID string, logIndex int) {
	raft.mu.Lock()
	if raft.nextIndex[serverID] > nextIndex {
		raft.nextIndex[serverID] = nextIndex
		DPrintf("把follower: %d的nextIndex改为: %d, traceId: %v.", serverID, nextIndex, traceID)
	}
	raft.mu.Unlock()

	// nextIndex是向follower发送log数组的界限，最小为零，而replicateToFollower的参数lowBound是prevLogIndex，所以可以为-1
	replicateToFollower(serverID, nextIndex-1, logIndex, raft, traceID)
}
