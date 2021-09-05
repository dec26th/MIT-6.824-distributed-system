package raft

import (
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
	return time.Duration(rand.Intn(to - from) + from) * time.Millisecond
}
