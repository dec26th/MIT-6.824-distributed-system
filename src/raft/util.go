package raft

import (
	"6.824/consts"
	"log"
	"math/rand"
	"time"
)

// Debugging
//const Debug = false
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func RandTimeMilliseconds(from, to int) time.Duration {
	part := (to - from) / consts.Interval

	return time.Duration(rand.Intn(part)*consts.Interval+from) * time.Millisecond
}

func init() {
	log.SetFlags(log.Lmicroseconds)
}
