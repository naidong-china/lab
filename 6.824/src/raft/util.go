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

func RandomizeSleep(server int, ms time.Duration) (res int) {
	if ms != 0 {
		time.Sleep(ms)
		return int(ms.Milliseconds())
	}

	rand.Seed(time.Now().UnixNano() + int64(server))
	min, max := 30, 150
	for res < max {
		res = rand.Intn(max)
		if min < res {
			break
		}
	}
	time.Sleep(time.Duration(res) * time.Millisecond)
	return res
}
