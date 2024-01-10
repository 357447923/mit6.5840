package raft

import (
	"fmt"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int) {
	if Debug {
		fmt.Printf(format, a...)
	}
	return
}
