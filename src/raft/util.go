package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
)

// Debugging
const Debug = true

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

func DPrintf(format string, a ...interface{}) (n int) {
	if Debug {
		fmt.Printf(format, a...)
	}
	return
}

func (rf *Raft) GenLogIdx(cmdIdx int) (logIdx int) {
	if rf.snapshot[0].SnapshotValid {
		logIdx = cmdIdx - rf.snapshot[0].SnapshotIndex - 1
	} else {
		logIdx = cmdIdx - 1
	}
	if logIdx < 0 {
		DPrintf("server=%d gen idx=%d, cmdIdx=%d, rf.snapshot valid=%v, index=%d, term=%d\n",
			rf.id, logIdx, cmdIdx, rf.snapshot[0].SnapshotValid, rf.snapshot[0].SnapshotIndex, rf.snapshot[0].SnapshotTerm)
	}
	return
}
