package shipyard

import (
	"Mix/dlog"
	"Mix/state"
)

//const TruncateSize = 512 * 1024

type TruncatedLog struct {
	log                []LogEntry
	logSize            int32 // Absolute logsize
	offset             int32 // Absolute index
	lastTruncatedIndex int32
	truncateSize       int32
}

func NewTruncatedLog(capacity int) *TruncatedLog {
	return &TruncatedLog{
		log:                make([]LogEntry, 0, capacity),
		logSize:            0,
		offset:             0,
		lastTruncatedIndex: 0,
		truncateSize:       int32(capacity / 10),
	}
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
	relative := index - t.offset
	if relative < 0 {
		dlog.Error("Set: invalid index %v, offset %v, capacity %v", index, t.offset, len(t.log))
		return
	}
	if relative >= int32(len(t.log)) {
		t.log = append(t.log, make([]LogEntry, relative+1-int32(len(t.log)))...)
	}
	t.log[relative] = entry
}

func (t *TruncatedLog) GetTerm(index int32) (int32, int32) {
	if index < t.offset || index-t.offset >= int32(len(t.log)) {
		return 0, 0
	}
	return t.log[index-t.offset].Epoch, t.log[index-t.offset].Apportion
}

func (t *TruncatedLog) Get(index int32) LogEntry {
	if index < t.offset {
		lastEpoch, lastApportion := int32(0), int32(0)
		if t.lastTruncatedIndex >= 0 {
			lastEpoch, lastApportion = t.GetTerm(t.lastTruncatedIndex)
		}
		return LogEntry{
			Epoch:     lastEpoch,
			Apportion: lastApportion,
			Command:   state.Command{Op: state.PUT, K: state.Key(0), V: state.Truncated},
		}
	}
	relative := index - t.offset
	if relative >= int32(len(t.log)) {
		dlog.Error("Get: invalid index %v, offset %v, logSize %v", index, t.offset, t.logSize)
		return LogEntry{}
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
		lastEpoch, lastApportion := int32(0), int32(0)
		if t.lastTruncatedIndex >= 0 {
			lastEpoch, lastApportion = t.GetTerm(t.lastTruncatedIndex)
		}
		for i := int32(0); i < truncatedLen; i++ {
			entries[i] = LogEntry{
				Epoch:     lastEpoch,
				Apportion: lastApportion,
				Command:   state.Command{Op: state.PUT, K: state.Key(0), V: state.Truncated},
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
}
