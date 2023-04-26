package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = true

func init() {
	log.SetFlags(log.Ldate | log.Lmicroseconds)
}

func DPrintf(format string, a ...interface{}) {
	if Debug {
		format += "\n"
		log.Printf(format, a...)
	}
	return
}

func Majority(n int) int {
	if n%2 == 0 {
		return n/2 + 1
	} else {
		return (n + 1) / 2
	}
}

func RandomizeSleepTime(min, max int) time.Duration {
	rand.Seed(time.Now().Unix())
	res := 0
	for res < max {
		res = rand.Intn(max)
		if min < res {
			return time.Duration(res) * time.Millisecond
		}
	}
	return time.Duration(0)
}
