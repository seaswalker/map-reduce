package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"

	"github.com/seaswalker/map-reduce/labrpc"

	"github.com/seaswalker/map-reduce/raft"

	"github.com/seaswalker/map-reduce/labgob"
)

const Debug = 1
const loseLeadershipIndex = -1

var redisInitFlag int32

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
	ClientID  int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	dataStore map[string]string
	// RPC handler监听某个index的日志提交
	logCommitListener         map[int]chan logCommitListenReply
	logCommitListenerLock     sync.Mutex
	stopLogCommitListenerChan chan bool
	persister                 *raft.Persister
	// key为客户端id
	duplicatedRequestFilter map[int64]response
}

type logCommitListenReply struct {
	index int
	// 只有get请求才不为空
	value string
}

// snapshot KVServer需要保存到快照中的属性
type snapshot struct {
	DataStore               map[string]string
	DuplicatedRequestFilter map[int64]response
}

// 一次请求-响应
type response struct {
	RequestID int64
	Resp      string
}

// Get 强一致性读
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("KVServer: %d收到请求: %d.", kv.me, args.ID)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	command := &Op{Operation: "Get", Key: args.Key, RequestID: args.ID}

	kv.logCommitListenerLock.Lock()
	index, _, success := kv.rf.Start(command)
	DPrintf("%d向Raft提交Get操作, id: %d, key: %s, 结果: %v.", kv.me, args.ID, args.Key, success)

	if !success {
		reply.WrongLeader = true
		kv.logCommitListenerLock.Unlock()
		return
	}

	ch := make(chan logCommitListenReply)
	registerLogCommitListener(kv, index, ch)
	kv.logCommitListenerLock.Unlock()

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

	// 保证同时只有一个请求在被处理
	kv.mu.Lock()
	defer kv.mu.Unlock()

	command := &Op{Operation: args.Op, Key: args.Key, Value: args.Value, RequestID: args.ID, ClientID: args.ClientID}

	// 防止在调用start方法和注册监听器之间收到失去leadership消息，导致永远阻塞客户端请求
	kv.logCommitListenerLock.Lock()
	index, _, success := kv.rf.Start(command)
	DPrintf("%d向Raft提交%s请求, id: %d, key: %s, value: %s, 结果: %v.", kv.me, args.Op, args.ID, args.Key, args.Value, success)

	if !success {
		reply.WrongLeader = true
		kv.logCommitListenerLock.Unlock()
		return
	}

	DPrintf("%d开始等待%d提交, 请求ID: %d.", kv.me, index, args.ID)

	ch := make(chan logCommitListenReply)
	registerLogCommitListener(kv, index, ch)
	kv.logCommitListenerLock.Unlock()

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
	kv.stopLogCommitListenerChan <- true
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
	labgob.Register(&snapshot{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.stopLogCommitListenerChan = make(chan bool)
	kv.persister = persister
	kv.logCommitListener = make(map[int]chan logCommitListenReply)
	kv.duplicatedRequestFilter = make(map[int64]response)
	kv.dataStore = make(map[string]string)
	go listenRaftLogCommit(kv)

	// You may need initialization code here.
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	return kv
}

func registerLogCommitListener(kv *KVServer, index int, ch chan logCommitListenReply) {
	if kv.logCommitListener[index] != nil {
		panic("对index: " + strconv.Itoa(index) + "的监听器已存在!")
	}
	kv.logCommitListener[index] = ch
}

func listenRaftLogCommit(kv *KVServer) {
	for {
		select {
		case applyMessage := <-kv.applyCh:
			if applyMessage == raft.LOSELEADERSHIPAPPLYMESSAGE {
				notifyLoseLeadership(kv)
				continue
			}

			command := applyMessage.Command
			_, ok := command.(*Op)
			if ok {
				handleApplyMessage(kv, applyMessage)
				continue
			}

			data, ok := command.([]byte)
			if ok {
				overwriteLocalDatastore(data, kv)
				continue
			}
		case _ = <-kv.stopLogCommitListenerChan:
			break
		}
	}
}

func handleApplyMessage(kv *KVServer, applyMessage raft.ApplyMsg) {
	op := applyMessage.Command.(*Op)

	var value string

	resp, ok := kv.duplicatedRequestFilter[op.ClientID]
	if ok && resp.RequestID == op.RequestID {
		DPrintf("KVServer: %d发现client: %d的重复请求: %d.", kv.me, op.ClientID, op.RequestID)
		value = resp.Resp
	} else {
		// update操作不关心返回值，value是空串
		value = applyCommand(op, kv)
		kv.duplicatedRequestFilter[op.ClientID] = response{RequestID: op.RequestID, Resp: value}
	}

	notifyLogCommit(applyMessage, value, op, kv)

	createSnapshotIfNecessary(kv, applyMessage.CommandIndex)
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

func notifyLogCommit(applyMessage raft.ApplyMsg, value string, op *Op, kv *KVServer) {
	kv.logCommitListenerLock.Lock()
	defer kv.logCommitListenerLock.Unlock()

	ch := kv.logCommitListener[applyMessage.CommandIndex]
	if ch == nil {
		return
	}

	ch <- logCommitListenReply{index: applyMessage.CommandIndex, value: value}
	delete(kv.logCommitListener, applyMessage.CommandIndex)
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

func createSnapshotIfNecessary(kv *KVServer, index int) {
	if kv.maxraftstate <= 0 || kv.maxraftstate > kv.persister.RaftStateSize() {
		return
	}

	snap := &snapshot{DataStore: kv.dataStore, DuplicatedRequestFilter: kv.duplicatedRequestFilter}

	data := labgob.EncodeToByteArray(snap)
	kv.rf.CreateSnapshot(data, index)
}

// 用leader发送过来的snapshot覆盖本地的数据
func overwriteLocalDatastore(snapshotData []byte, kv *KVServer) {
	reader := bytes.NewBuffer(snapshotData)
	decoder := labgob.NewDecoder(reader)

	var snap = &snapshot{}
	err := decoder.Decode(snap)
	if err != nil {
		panic(err)
	}

	kv.dataStore = snap.DataStore
	kv.duplicatedRequestFilter = snap.DuplicatedRequestFilter

	DPrintf("%d收到snapshot覆盖请求, dataStore: %#v.", kv.me, kv.dataStore)
}
