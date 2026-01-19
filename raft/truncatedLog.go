package raft

import (
	"Mix/dlog"
	"Mix/state"
	"fmt"
)

//const TruncateSize = 512 * 1024

type TruncatedLog struct {
	log                []LogEntry
	logSize            int32 // Absolute logsize
	offset             int32 // Absolute index
	id                 int32
	lastTruncatedIndex int32
	truncateSize       int32
}

func NewTruncatedLog(id int32, capacity int) *TruncatedLog {
	var logSize int32 = dlog.ReadLog(id)
	t := &TruncatedLog{
		log:                make([]LogEntry, 0, capacity),
		truncateSize:       int32(capacity / 10),
		logSize:            logSize,
		offset:             logSize,
		lastTruncatedIndex: logSize,
		id:                 id,
	}
	return t
}

func (t *TruncatedLog) Size() int32 {
	return t.logSize
}

func (t *TruncatedLog) SetSize(size int32) {
	t.logSize = size
}

func (t *TruncatedLog) IncSize() {
	t.logSize++
}

func (t *TruncatedLog) Set(index int32, entry LogEntry) {
	if index < t.offset {
		// already truncated, ignore
		return
	}
	relative := index - t.offset
	if relative >= int32(len(t.log)) {
		t.log = append(t.log, make([]LogEntry, relative+1-int32(len(t.log)))...)
	}
	t.log[relative] = entry
}

func (t *TruncatedLog) GetTerm(index int32) int32 {
	if index < t.offset || index-t.offset >= int32(len(t.log)) {
		return 0
	}
	return t.log[index-t.offset].Term
}

func (t *TruncatedLog) Get(index int32) LogEntry {
	lastTerm := int32(0)
	if index < t.offset {
		if t.lastTruncatedIndex >= 0 {
			lastTerm = t.GetTerm(t.lastTruncatedIndex)
		}
		return LogEntry{
			Term:    lastTerm,
			Command: state.Command{Op: state.PUT, K: state.Key(0), V: state.Truncated},
		}
	}
	relative := index - t.offset
	if relative >= int32(len(t.log)) {
		dummy := LogEntry{
			Term:    lastTerm,
			Command: state.Command{Op: state.PUT, K: state.Key(0), V: state.Truncated}, // or dummy command
		}
		return dummy
	}
	return t.log[relative]
}

func (t *TruncatedLog) Append(entry LogEntry) {
	if t.logSize-t.offset >= int32(len(t.log)) {
		dlog.Error("Append: log capacity exceeded, log size %v, offset %v, capacity %v", t.logSize, t.offset, len(t.log))
		return
	}
	t.log[t.logSize-t.offset] = entry
	t.logSize++
}

func (t *TruncatedLog) Slice(start, end int32) []LogEntry {
	if end > t.logSize {
		end = t.logSize
	}
	if start > end {
		return []LogEntry{}
	}

	relativeStart := start - t.offset
	relativeEnd := end - t.offset

	if relativeStart < 0 {
		dlog.Printf("Slice: truncated range [%v:%v], offset %v → fill with truncated entries", start, end, t.offset)
		truncatedLen := -relativeStart
		if relativeEnd < 0 {
			relativeEnd = 0
		}
		entries := make([]LogEntry, truncatedLen+relativeEnd)
		lastTerm := int32(0)
		if t.lastTruncatedIndex >= 0 {
			lastTerm = t.GetTerm(t.lastTruncatedIndex)
		}
		for i := int32(0); i < truncatedLen; i++ {
			entries[i] = LogEntry{
				Term:    lastTerm,
				Command: state.Command{Op: state.PUT, K: state.Key(0), V: state.Truncated},
			}
		}
		copy(entries[truncatedLen:], t.log[:relativeEnd])
		return entries
	}

	if relativeEnd > int32(len(t.log)) {
		relativeEnd = int32(len(t.log))
	}
	return t.log[relativeStart:relativeEnd]
}

func (t *TruncatedLog) TruncateIfNeeded(to int32) {
	if to-t.offset > 2*t.truncateSize {
		t.log = t.log[t.truncateSize:]
		t.offset += t.truncateSize
	}
	t.Update()
}

func (t *TruncatedLog) Update() {
	size := fmt.Sprintf("%v", t.logSize)
	dlog.StoreLog(t.id, size)
}
