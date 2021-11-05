package raft

import (
	"log"
	"math/rand"
	"time"

	"6.824/consts"
)

// Debugging
const Debug = false

// const Debug = true

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
