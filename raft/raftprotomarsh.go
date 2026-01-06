package raft

import (
	"Mix/fastrpc"
	"bufio"
	"encoding/binary"
	"io"
	"sync"
)

type byteReader interface {
	io.Reader
	ReadByte() (c byte, err error)
}

/***** RequestVoteArgs *****/

func (t *RequestVoteArgs) New() fastrpc.Serializable { return new(RequestVoteArgs) }
func (t *RequestVoteArgs) BinarySize() (nbytes int, sizeKnown bool) {
	// contains variable-length strings -> size unknown
	return 0, false
}

type RequestVoteArgsCache struct {
	mu    sync.Mutex
	cache []*RequestVoteArgs
}

func NewRequestVoteArgsCache() *RequestVoteArgsCache {
	c := &RequestVoteArgsCache{}
	c.cache = make([]*RequestVoteArgs, 0)
	return c
}

func (p *RequestVoteArgsCache) Get() *RequestVoteArgs {
	var t *RequestVoteArgs
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &RequestVoteArgs{}
	}
	return t
}

func (p *RequestVoteArgsCache) Put(t *RequestVoteArgs) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}

func (t *RequestVoteArgs) Marshal(wire io.Writer) {
	// Order in struct:
	// Term int32
	// CandidateId string
	// LastLogIndex int32
	// LastLogTerm int32
	// Group string

	var b [12]byte
	var bs []byte

	// write Term (4 bytes)
	bs = b[:4]
	tmp32 := t.Term
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)

	// write CandidateId (int32)
	bs = b[:4]
	tmp32 = t.CandidateId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)

	// write LastLogIndex and LastLogTerm (8 bytes)
	bs = b[:8]
	tmp32 = t.LastLogIndex
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.LastLogTerm
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	wire.Write(bs)

	// write Shard (int32)
	bs = b[:4]
	tmp32 = t.Shard
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *RequestVoteArgs) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}

	// read Term (4 bytes)
	var b4 [4]byte
	bs4 := b4[:4]
	if _, err := io.ReadAtLeast(wire, bs4, 4); err != nil {
		return err
	}
	t.Term = int32((uint32(bs4[0]) | (uint32(bs4[1]) << 8) | (uint32(bs4[2]) << 16) | (uint32(bs4[3]) << 24)))

	// read CandidateId (int32)
	var b4_2 [4]byte
	if _, err := io.ReadAtLeast(wire, b4_2[:], 4); err != nil {
		return err
	}
	t.CandidateId = int32(uint32(b4_2[0]) |
		uint32(b4_2[1])<<8 |
		uint32(b4_2[2])<<16 |
		uint32(b4_2[3])<<24)

	// read LastLogIndex and LastLogTerm (8 bytes)
	var b8 [8]byte
	bs8 := b8[:8]
	if _, err := io.ReadAtLeast(wire, bs8, 8); err != nil {
		return err
	}
	t.LastLogIndex = int32((uint32(bs8[0]) | (uint32(bs8[1]) << 8) | (uint32(bs8[2]) << 16) | (uint32(bs8[3]) << 24)))
	t.LastLogTerm = int32((uint32(bs8[4]) | (uint32(bs8[5]) << 8) | (uint32(bs8[6]) << 16) | (uint32(bs8[7]) << 24)))

	// read Shard (int32)
	var b4_3 [4]byte
	if _, err := io.ReadAtLeast(wire, b4_3[:], 4); err != nil {
		return err
	}
	t.Shard = int32(uint32(b4_3[0]) |
		uint32(b4_3[1])<<8 |
		uint32(b4_3[2])<<16 |
		uint32(b4_3[3])<<24)

	return nil
}

/***** RequestVoteReply *****/

func (t *RequestVoteReply) New() fastrpc.Serializable { return new(RequestVoteReply) }
func (t *RequestVoteReply) BinarySize() (nbytes int, sizeKnown bool) {
	// contains Group string -> size unknown
	return 0, false
}

type RequestVoteReplyCache struct {
	mu    sync.Mutex
	cache []*RequestVoteReply
}

func NewRequestVoteReplyCache() *RequestVoteReplyCache {
	c := &RequestVoteReplyCache{}
	c.cache = make([]*RequestVoteReply, 0)
	return c
}

func (p *RequestVoteReplyCache) Get() *RequestVoteReply {
	var t *RequestVoteReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &RequestVoteReply{}
	}
	return t
}

func (p *RequestVoteReplyCache) Put(t *RequestVoteReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}

func (t *RequestVoteReply) Marshal(wire io.Writer) {
	// Term int32
	// VoteGranted bool
	// Group string

	var b [9]byte
	var bs []byte

	// write Term (4 bytes)
	bs = b[:4]
	tmp32 := t.Term
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)

	// write VoteGranted (1 byte)
	var bb [1]byte
	if t.VoteGranted {
		bb[0] = byte(1)
	} else {
		bb[0] = byte(0)
	}
	wire.Write(bb[:1])

	// write Shard (varint length + bytes)
	bs = b[:4]
	tmp32 = t.Shard
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *RequestVoteReply) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}

	// read Term (4 bytes)
	var b4 [4]byte
	bs4 := b4[:4]
	if _, err := io.ReadAtLeast(wire, bs4, 4); err != nil {
		return err
	}
	t.Term = int32((uint32(bs4[0]) | (uint32(bs4[1]) << 8) | (uint32(bs4[2]) << 16) | (uint32(bs4[3]) << 24)))

	// read VoteGranted (1 byte)
	var b1 [1]byte
	if _, err := io.ReadAtLeast(wire, b1[:], 1); err != nil {
		return err
	}
	t.VoteGranted = (b1[0] != 0)

	// read Shard (4 bytes)
	bs4 = b4[:4]
	if _, err := io.ReadAtLeast(wire, bs4, 4); err != nil {
		return err
	}
	t.Shard = int32((uint32(bs4[0]) | (uint32(bs4[1]) << 8) | (uint32(bs4[2]) << 16) | (uint32(bs4[3]) << 24)))

	return nil
}

/***** AppendEntriesArgs *****/

func (t *AppendEntriesArgs) New() fastrpc.Serializable { return new(AppendEntriesArgs) }
func (t *AppendEntriesArgs) BinarySize() (nbytes int, sizeKnown bool) {
	// contains variable-length strings and []role.Command -> size unknown
	return 0, false
}

type AppendEntriesArgsCache struct {
	mu    sync.Mutex
	cache []*AppendEntriesArgs
}

func NewAppendEntriesArgsCache() *AppendEntriesArgsCache {
	c := &AppendEntriesArgsCache{}
	c.cache = make([]*AppendEntriesArgs, 0)
	return c
}

func (p *AppendEntriesArgsCache) Get() *AppendEntriesArgs {
	var t *AppendEntriesArgs
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &AppendEntriesArgs{}
	}
	return t
}

func (p *AppendEntriesArgsCache) Put(t *AppendEntriesArgs) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}

func (t *AppendEntriesArgs) Marshal(wire io.Writer) {
	// Order:
	// Term int32
	// LeaderId string
	// PrevLogIndex int32
	// PrevLogTerm int32
	// Entries []role.Command
	// LeaderCommit int32
	// Group string

	var b [12]byte
	var bs []byte

	// write Term (4 bytes)
	bs = b[:4]
	tmp32 := t.Term
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)

	// write LeaderId (int32)
	bs = b[:4]
	tmp32 = t.LeaderId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)

	// write PrevLogIndex and PrevLogTerm (8 bytes)
	bs = b[:8]
	tmp32 = t.PrevLogIndex
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.PrevLogTerm
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	wire.Write(bs)

	//// write Entries (varint length + each entry Marshal)
	//bs = b[:]
	//alen2 := int64(len(t.Entries))
	//if wlen := binary.PutVarint(bs, alen2); wlen >= 0 {
	//	wire.Write(b[0:wlen])
	//}
	//for i := int64(0); i < alen2; i++ {
	//	t.Entries[i].Marshal(wire)
	//}

	// Marshal Entries ([]LogEntry)
	bs = b[:]

	// write length (varint)
	alen := int64(len(t.Entries))
	if wlen := binary.PutVarint(bs, alen); wlen >= 0 {
		wire.Write(b[:wlen])
	}

	// write each LogEntry
	for i := int64(0); i < alen; i++ {
		// 1. Command
		t.Entries[i].Command.Marshal(wire)

		// 2. Term (int32)
		var termBuf [4]byte
		binary.LittleEndian.PutUint32(termBuf[:], uint32(t.Entries[i].Term))
		wire.Write(termBuf[:])
	}

	// write LeaderCommit (4 bytes)
	bs = b[:4]
	tmp32 = t.LeaderCommit
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)

	// write Shard (varint length + bytes)
	bs = b[:4]
	tmp32 = t.Shard
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *AppendEntriesArgs) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}

	// read Term (4 bytes)
	var b4 [4]byte
	if _, err := io.ReadAtLeast(wire, b4[:], 4); err != nil {
		return err
	}
	t.Term = int32((uint32(b4[0]) | (uint32(b4[1]) << 8) | (uint32(b4[2]) << 16) | (uint32(b4[3]) << 24)))

	// read LeaderId (int32)
	var b4_3 [4]byte
	if _, err := io.ReadAtLeast(wire, b4_3[:], 4); err != nil {
		return err
	}
	t.LeaderId = int32(uint32(b4_3[0]) |
		uint32(b4_3[1])<<8 |
		uint32(b4_3[2])<<16 |
		uint32(b4_3[3])<<24)

	// read PrevLogIndex and PrevLogTerm (8 bytes)
	var b8 [8]byte
	if _, err := io.ReadAtLeast(wire, b8[:], 8); err != nil {
		return err
	}
	t.PrevLogIndex = int32((uint32(b8[0]) | (uint32(b8[1]) << 8) | (uint32(b8[2]) << 16) | (uint32(b8[3]) << 24)))
	t.PrevLogTerm = int32((uint32(b8[4]) | (uint32(b8[5]) << 8) | (uint32(b8[6]) << 16) | (uint32(b8[7]) << 24)))

	// read Entries (varint length + each entry Unmarshal)
	//alen2, err := binary.ReadVarint(wire)
	//if err != nil {
	//	return err
	//}
	//t.Entries = make([]role.Command, alen2)
	//for i := int64(0); i < alen2; i++ {
	//	t.Entries[i].Unmarshal(wire)
	//}

	// Unmarshal Entries ([]LogEntry)
	// read length (varint)
	alen, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}

	t.Entries = make([]LogEntry, alen)

	for i := int64(0); i < alen; i++ {
		// 1. Command
		if err := t.Entries[i].Command.Unmarshal(wire); err != nil {
			return err
		}

		// 2. Term
		var termBuf [4]byte
		if _, err := io.ReadFull(wire, termBuf[:]); err != nil {
			return err
		}
		t.Entries[i].Term = int32(binary.LittleEndian.Uint32(termBuf[:]))
	}

	// read LeaderCommit (4 bytes)
	var b4b [4]byte
	if _, err := io.ReadAtLeast(wire, b4b[:], 4); err != nil {
		return err
	}
	t.LeaderCommit = int32((uint32(b4b[0]) | (uint32(b4b[1]) << 8) | (uint32(b4b[2]) << 16) | (uint32(b4b[3]) << 24)))

	// read Shard (4 bytes)
	var b4c [4]byte
	if _, err := io.ReadAtLeast(wire, b4c[:], 4); err != nil {
		return err
	}
	t.Shard = int32((uint32(b4c[0]) | (uint32(b4c[1]) << 8) | (uint32(b4c[2]) << 16) | (uint32(b4c[3]) << 24)))

	return nil
}

/***** AppendEntriesReply *****/

func (t *AppendEntriesReply) New() fastrpc.Serializable { return new(AppendEntriesReply) }
func (t *AppendEntriesReply) BinarySize() (nbytes int, sizeKnown bool) {
	// contains Group string -> size unknown
	return 0, false
}

type AppendEntriesReplyCache struct {
	mu    sync.Mutex
	cache []*AppendEntriesReply
}

func NewAppendEntriesReplyCache() *AppendEntriesReplyCache {
	c := &AppendEntriesReplyCache{}
	c.cache = make([]*AppendEntriesReply, 0)
	return c
}

func (p *AppendEntriesReplyCache) Get() *AppendEntriesReply {
	var t *AppendEntriesReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &AppendEntriesReply{}
	}
	return t
}

func (p *AppendEntriesReplyCache) Put(t *AppendEntriesReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}

func (t *AppendEntriesReply) Marshal(wire io.Writer) {
	// Term int32
	// Success bool
	// ConflictIndex int32
	// ConflictTerm int32
	// Group string

	var b [12]byte
	var bs []byte

	// write Term (4 bytes)
	bs = b[:4]
	tmp32 := t.Term
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)

	// write Success (1 byte)
	var b1 [1]byte
	if t.Success {
		b1[0] = byte(1)
	} else {
		b1[0] = byte(0)
	}
	wire.Write(b1[:1])

	// write FollowerId and LastLogIndex (8 bytes)
	bs = b[:8]
	tmp32 = t.FollowerId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.LastLogIndex
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	wire.Write(bs)

	// write Shard (varint length + bytes)
	bs = b[:4]
	tmp32 = t.Shard
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *AppendEntriesReply) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}

	// read Term (4 bytes)
	var b4 [4]byte
	if _, err := io.ReadAtLeast(wire, b4[:], 4); err != nil {
		return err
	}
	t.Term = int32((uint32(b4[0]) | (uint32(b4[1]) << 8) | (uint32(b4[2]) << 16) | (uint32(b4[3]) << 24)))

	// read Success (1 byte)
	var b1 [1]byte
	if _, err := io.ReadAtLeast(wire, b1[:], 1); err != nil {
		return err
	}
	t.Success = (b1[0] != 0)

	// read FollowerId and LastLogIndex (8 bytes)
	var b8 [8]byte
	if _, err := io.ReadAtLeast(wire, b8[:], 8); err != nil {
		return err
	}
	t.FollowerId = int32((uint32(b8[0]) | (uint32(b8[1]) << 8) | (uint32(b8[2]) << 16) | (uint32(b8[3]) << 24)))
	t.LastLogIndex = int32((uint32(b8[4]) | (uint32(b8[5]) << 8) | (uint32(b8[6]) << 16) | (uint32(b8[7]) << 24)))

	// read Shard (4 bytes)
	var b4s [4]byte
	if _, err := io.ReadAtLeast(wire, b4s[:], 4); err != nil {
		return err
	}
	t.Shard = int32((uint32(b4s[0]) | (uint32(b4s[1]) << 8) | (uint32(b4s[2]) << 16) | (uint32(b4s[3]) << 24)))

	return nil
}
