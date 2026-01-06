package shipyard

import "Mix/dlog"

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

func (t *TruncatedLog) Get(index int32) LogEntry {
	relative := index - t.offset
	if relative < 0 || relative >= t.logSize-t.offset {
		dlog.Error("Get: invalid index %v, offset %v, currentIndex %v", index, t.offset, t.logSize-t.offset)
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
	relativeStart := start - t.offset
	relativeEnd := end - t.offset

	if relativeStart < 0 || end > t.logSize || relativeStart > relativeEnd {
		dlog.Error("Slice: invalid range [%v:%v], offset %v, log size %v", start, end, t.offset, t.logSize)
		return []LogEntry{}
	}

	return t.log[relativeStart:relativeEnd]
}

func (t *TruncatedLog) TruncateIfNeeded(to int32) {
	if to-t.offset > 2*t.truncateSize {
		t.log = t.log[t.truncateSize:]
		t.offset += t.truncateSize
	}
	//if to-t.offset > 2*t.truncateSize {
	//	newLog := make([]LogEntry, len(t.log)-int(t.truncateSize))
	//	copy(newLog, t.log[t.truncateSize:])
	//	t.log = newLog
	//	t.offset += t.truncateSize
	//}
}
