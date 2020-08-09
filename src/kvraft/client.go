package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeaderIndex int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.lastLeaderIndex = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	if key == "" {
		return ""
	}
	getArgs := GetArgs{Key: key}
	result := ""
	for {
		reply := GetReply{}
		server := ck.servers[ck.lastLeaderIndex]
		success := server.Call("KVServer.Get", &getArgs, &reply)
		if !success || reply.WrongLeader || reply.Err != "" {
			ck.lastLeaderIndex++
			if ck.lastLeaderIndex == len(ck.servers) {
				ck.lastLeaderIndex = 0
			}
			continue
		}

		result = reply.Value
		break
	}
	return result
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{Key: key, Value: value, Op: op}

	for {
		reply := PutAppendReply{}
		server := ck.servers[ck.lastLeaderIndex]
		success := server.Call("KVServer.PutAppend", &args, &reply)
		if !success || reply.WrongLeader || reply.Err != "" {
			ck.lastLeaderIndex++
			if ck.lastLeaderIndex == len(ck.servers) {
				ck.lastLeaderIndex = 0
			}
			continue
		}

		break
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
