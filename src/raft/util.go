package raft

import "log"

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) {
	if Debug > 0 {
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
		log.Printf(format, a...)
	}
	return
}
