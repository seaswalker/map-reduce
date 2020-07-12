package raft

import "log"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) {
	if Debug > 0 {
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
		log.Printf(format, a...)
	}
	return
}

func isDebugEnabled() bool {
	return Debug > 0
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
