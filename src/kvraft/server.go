package raftkv

import (
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 1

// 全局(所有server共用)的请求过滤器，key为请求ID，如果ID在此map中存在，那么说明当前请求已被处理过，用以防止对同一个请求的重复处理
// 比如server实际上已经处理完，但client没有收到响应(网络超时)，在这种情形下client会重复发送请求
var filter map[int64]bool = make(map[int64]bool)

// 全局的数据存储
var dataStore map[string]string = make(map[string]string)

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
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("KVServer: %d收到请求: %d.", kv.me, args.ID)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	command := &Op{Operation: "Get", Key: args.Key}
	index, _, success := kv.rf.Start(command)
	DPrintf("%d向Raft提交Get操作, id: %d, key: %s, 结果: %v.", kv.me, args.ID, args.Key, success)

	if !success {
		reply.WrongLeader = true
		return
	}

	var applyMessage raft.ApplyMsg
	for {
		applyMessage = <-kv.applyCh
		if applyMessage != raft.LOSELEADERSHIPAPPLYMESSAGE && applyMessage.CommandIndex != index {
			continue
		}
		break
	}

	if applyMessage.Command == command {
		reply.Value = applyCommand(command, kv)
		DPrintf("%d get %s: %s, id: %d.", kv.me, args.Key, reply.Value, args.ID)
	} else {
		reply.Err = Err(fmt.Sprintf("Leader: %d has losed leadership.", kv.me))
		DPrintf("%d不再是leader, 请求: %d需要重试.", kv.me, args.ID)
	}
}

func applyCommand(op *Op, kv *KVServer) string {
	switch op.Operation {
	case "Get":
		return dataStore[op.Key]
	case "Put":
		dataStore[op.Key] = op.Value
		break
	case "Append":
		oldValue := dataStore[op.Key]
		if oldValue == "" {
			dataStore[op.Key] = op.Value
		} else {
			dataStore[op.Key] = oldValue + op.Value
		}
	}

	return ""
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("KVServer: %d收到请求: %d.", kv.me, args.ID)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 先检查当前server对应的raft状态，防止并发修改filter
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}

	if filter[args.ID] {
		return
	}

	command := &Op{Operation: args.Op, Key: args.Key, Value: args.Value}
	index, _, success := kv.rf.Start(command)
	DPrintf("%d向Raft提交%s请求, id: %d, key: %s, value: %s, 结果: %v.", kv.me, args.Op, args.ID, args.Key, args.Value, success)

	if !success {
		reply.WrongLeader = true
		return
	}

	DPrintf("%d开始等待%d提交, 请求ID: %d.", kv.me, index, args.ID)

	var applyMessage raft.ApplyMsg
	for {
		applyMessage = <-kv.applyCh
		if applyMessage != raft.LOSELEADERSHIPAPPLYMESSAGE && applyMessage.CommandIndex != index {
			continue
		}
		break
	}

	if applyMessage.Command == command {
		applyCommand(command, kv)
		DPrintf("%d将%s的值更改为: %s, id: %d, index: %d.", kv.me, args.Key, dataStore[args.Key], args.ID, index)
		filter[args.ID] = true
	} else {
		reply.Err = Err(fmt.Sprintf("Leader: %d has losed leadership.", kv.me))
		DPrintf("%d不再是leader, 请求: %d需要重试.", kv.me, args.ID)
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
	dataStore = nil
	filter = nil
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
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	if dataStore == nil {
		dataStore = make(map[string]string)
	}
	if filter == nil {
		filter = make(map[int64]bool)
	}
	return kv
}
