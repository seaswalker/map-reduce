package kvraft

import (
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

const Debug = 1
const loseLeadershipIndex = -1

var poison = raft.ApplyMsg{}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	Key       string
	Value     string
	RequestID int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	dataStore map[string]string
	// RPC handler监听某个index的日志提交
	logCommitListener      map[int]chan logCommitListenReply
	logCommitListenerLock  sync.Mutex
	isKilled               atomic.Value
	duplicateRequestFilter map[int64]string
}

type logCommitListenReply struct {
	index int
	// 只有get请求才不为空
	value string
}

// Get 强一致性读
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("KVServer: %d收到请求: %d.", kv.me, args.ID)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	command := &Op{Operation: "Get", Key: args.Key, RequestID: args.ID}
	index, _, success := kv.rf.Start(command)
	DPrintf("%d向Raft提交Get操作, id: %d, key: %s, 结果: %v.", kv.me, args.ID, args.Key, success)

	if !success {
		reply.WrongLeader = true
		return
	}

	ch := make(chan logCommitListenReply)
	registerLogCommitListener(kv, index, ch)

	DPrintf("%d注册index: %d的监听器, 请求ID: %d, 开始等待raft事件.", kv.me, index, args.ID)

	listenReply := <-ch

	if listenReply.index == loseLeadershipIndex {
		reply.Err = Err(fmt.Sprintf("Leader: %d has losed leadership.", kv.me))
		DPrintf("%d不再是leader, 请求: %d需要重试.", kv.me, args.ID)
	} else {
		reply.Value = listenReply.value
		DPrintf("%d get %s: %s, id: %d.", kv.me, args.Key, reply.Value, args.ID)
	}
}

// PutAppend 如果key存在，那么追加(字符串拼接)，反之保存即可
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("KVServer: %d收到请求: %d.", kv.me, args.ID)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 先检查当前server对应的raft状态，防止并发修改filter
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}

	kv.logCommitListenerLock.Lock()
	requestHandled := kv.duplicateRequestFilter[args.ID]
	kv.logCommitListenerLock.Unlock()

	if requestHandled != "" {
		DPrintf("%d拒绝重复请求%d.", kv.me, args.ID)
		return
	}

	command := &Op{Operation: args.Op, Key: args.Key, Value: args.Value, RequestID: args.ID}
	index, _, success := kv.rf.Start(command)
	DPrintf("%d向Raft提交%s请求, id: %d, key: %s, value: %s, 结果: %v.", kv.me, args.Op, args.ID, args.Key, args.Value, success)

	if !success {
		reply.WrongLeader = true
		return
	}

	DPrintf("%d开始等待%d提交, 请求ID: %d.", kv.me, index, args.ID)

	ch := make(chan logCommitListenReply)
	registerLogCommitListener(kv, index, ch)

	DPrintf("%d注册index: %d的监听器, 请求ID: %d, 开始等待raft事件.", kv.me, index, args.ID)

	listenReply := <-ch
	if listenReply.index == loseLeadershipIndex {
		reply.Err = Err(fmt.Sprintf("Leader: %d has losed leadership.", kv.me))
		DPrintf("%d不再是leader, 请求: %d需要重试.", kv.me, args.ID)
	} else {
		DPrintf("%d将%s的值更改为: %s, id: %d, index: %d.", kv.me, args.Key, listenReply.value, args.ID, index)
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.isKilled.Store(true)
	kv.applyCh <- poison
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(&Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.isKilled.Store(false)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.logCommitListener = make(map[int]chan logCommitListenReply)
	kv.duplicateRequestFilter = make(map[int64]string)
	kv.dataStore = make(map[string]string)
	go listenRaftLogCommit(kv)

	return kv
}

func registerLogCommitListener(kv *KVServer, index int, ch chan logCommitListenReply) {
	kv.logCommitListenerLock.Lock()
	if kv.logCommitListener[index] != nil {
		panic("对index: " + strconv.Itoa(index) + "的监听器已存在!")
	}
	kv.logCommitListener[index] = ch
	kv.logCommitListenerLock.Unlock()
}

func listenRaftLogCommit(kv *KVServer) {
	for !kv.isKilled.Load().(bool) {
		applyMessage := <-kv.applyCh

		if applyMessage == poison {
			break
		}

		if applyMessage == raft.LOSELEADERSHIPAPPLYMESSAGE {
			notifyLoseLeadership(kv)
			continue
		}

		op := applyMessage.Command.(*Op)

		value := requestApplied(op.RequestID, kv)
		if value == "" {
			value = applyCommand(op, kv)
		}
		notifyLogCommit(applyMessage.CommandIndex, value, op.RequestID, kv)
	}
}

func notifyLoseLeadership(kv *KVServer) {
	kv.logCommitListenerLock.Lock()
	defer kv.logCommitListenerLock.Unlock()

	notifiedIndexes := make([]string, len(kv.logCommitListener))

	for index, ch := range kv.logCommitListener {
		ch <- logCommitListenReply{index: loseLeadershipIndex}
		notifiedIndexes = append(notifiedIndexes, strconv.Itoa(index))
	}

	kv.logCommitListener = make(map[int]chan logCommitListenReply)
	DPrintf("%d已向监听器: [%s]发送lose leadership通知.", kv.me, strings.Join(notifiedIndexes, ","))
}

func notifyLogCommit(index int, value string, requestID int64, kv *KVServer) {
	kv.logCommitListenerLock.Lock()
	defer kv.logCommitListenerLock.Unlock()

	kv.duplicateRequestFilter[requestID] = value

	ch := kv.logCommitListener[index]
	if ch == nil {
		return
	}

	ch <- logCommitListenReply{index: index, value: value}
	delete(kv.logCommitListener, index)
}

func requestApplied(requestID int64, kv *KVServer) string {
	kv.logCommitListenerLock.Lock()
	defer kv.logCommitListenerLock.Unlock()
	return kv.duplicateRequestFilter[requestID]
}

func applyCommand(op *Op, kv *KVServer) string {
	switch op.Operation {
	case "Get":
		break
	case "Put":
		kv.dataStore[op.Key] = op.Value
		break
	case "Append":
		oldValue := kv.dataStore[op.Key]
		if oldValue == "" {
			kv.dataStore[op.Key] = op.Value
		} else {
			kv.dataStore[op.Key] = oldValue + op.Value
		}
	}

	return kv.dataStore[op.Key]
}
