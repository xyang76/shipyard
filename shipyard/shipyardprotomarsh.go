package shipyard

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

/***** HeartbeatArgs *****/

func (t *HeartbeatArgs) New() fastrpc.Serializable { return new(HeartbeatArgs) }
func (t *HeartbeatArgs) BinarySize() (nbytes int, sizeKnown bool) {
	// contains variable-length slice -> size unknown
	return 0, false
}

type HeartbeatArgsCache struct {
	mu    sync.Mutex
	cache []*HeartbeatArgs
}

func NewHeartbeatArgsCache() *HeartbeatArgsCache {
	c := &HeartbeatArgsCache{}
	c.cache = make([]*HeartbeatArgs, 0)
	return c
}

func (p *HeartbeatArgsCache) Get() *HeartbeatArgs {
	var t *HeartbeatArgs
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0 : len(p.cache)-1]
	}
	p.mu.Unlock()
	if t == nil {
		t = &HeartbeatArgs{}
	}
	return t
}

func (p *HeartbeatArgsCache) Put(t *HeartbeatArgs) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}

func (t *HeartbeatArgs) Marshal(wire io.Writer) {
	var b [12]byte
	var bs []byte

	// write length of Shards (varint)
	bs = b[:]
	alen := int64(len(t.Shards))
	if wlen := binary.PutVarint(bs, alen); wlen >= 0 {
		wire.Write(bs[:wlen])
	}

	// write each Shard (int32)
	for i := int64(0); i < alen; i++ {
		bs = b[:4]
		tmp32 := t.Shards[i]
		bs[0] = byte(tmp32)
		bs[1] = byte(tmp32 >> 8)
		bs[2] = byte(tmp32 >> 16)
		bs[3] = byte(tmp32 >> 24)
		wire.Write(bs)
	}

	// write Apportion (int32)
	bs = b[:4]
	tmp32 := t.Apportion
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)

	// write Sender (int32)
	bs = b[:4]
	tmp32 = t.Sender
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *HeartbeatArgs) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}

	// read length of Shards (varint)
	alen, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}

	t.Shards = make([]int32, alen)
	var b4 [4]byte

	// read each Shard
	for i := int64(0); i < alen; i++ {
		if _, err := io.ReadAtLeast(wire, b4[:], 4); err != nil {
			return err
		}
		t.Shards[i] = int32(uint32(b4[0]) | uint32(b4[1])<<8 | uint32(b4[2])<<16 | uint32(b4[3])<<24)
	}

	// read Apportion
	if _, err := io.ReadAtLeast(wire, b4[:], 4); err != nil {
		return err
	}
	t.Apportion = int32(uint32(b4[0]) | uint32(b4[1])<<8 | uint32(b4[2])<<16 | uint32(b4[3])<<24)

	// read Sender
	if _, err := io.ReadAtLeast(wire, b4[:], 4); err != nil {
		return err
	}
	t.Sender = int32(uint32(b4[0]) | uint32(b4[1])<<8 | uint32(b4[2])<<16 | uint32(b4[3])<<24)

	return nil
}

/***** HeartbeatReply *****/

func (t *HeartbeatReply) New() fastrpc.Serializable { return new(HeartbeatReply) }
func (t *HeartbeatReply) BinarySize() (nbytes int, sizeKnown bool) {
	// fixed-size field: OK (1 byte)
	return 1, true
}

type HeartbeatReplyCache struct {
	mu    sync.Mutex
	cache []*HeartbeatReply
}

func NewHeartbeatReplyCache() *HeartbeatReplyCache {
	c := &HeartbeatReplyCache{}
	c.cache = make([]*HeartbeatReply, 0)
	return c
}

func (p *HeartbeatReplyCache) Get() *HeartbeatReply {
	var t *HeartbeatReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0 : len(p.cache)-1]
	}
	p.mu.Unlock()
	if t == nil {
		t = &HeartbeatReply{}
	}
	return t
}

func (p *HeartbeatReplyCache) Put(t *HeartbeatReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}

func (t *HeartbeatReply) Marshal(wire io.Writer) {
	// write OK (1 byte)
	var b1 [1]byte
	if t.OK {
		b1[0] = byte(1)
	} else {
		b1[0] = byte(0)
	}
	wire.Write(b1[:1])
}

func (t *HeartbeatReply) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}

	var b1 [1]byte
	if _, err := io.ReadAtLeast(wire, b1[:], 1); err != nil {
		return err
	}
	t.OK = (b1[0] != 0)

	return nil
}

/***** VoteAndGatherArgs *****/
func (t *VoteAndGatherArgs) New() fastrpc.Serializable { return new(VoteAndGatherArgs) }
func (t *VoteAndGatherArgs) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}
func (t *VoteAndGatherArgs) Marshal(wire io.Writer) {
	var b [4]byte
	var tmp32 int32
	var bs []byte

	// write Epoch (int32)
	bs = b[:4]
	tmp32 = t.Epoch
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)

	// write Apportion (int32)
	bs = b[:4]
	tmp32 = t.Apportion
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)

	// write Shard (int32)
	bs = b[:4]
	tmp32 = t.Shard
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

	// write CandidateCommit (int32)
	bs = b[:4]
	tmp32 = t.CandidateCommit
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *VoteAndGatherArgs) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}

	var b4 [4]byte
	// read Epoch
	if _, err := io.ReadAtLeast(wire, b4[:], 4); err != nil {
		return err
	}
	t.Epoch = int32(uint32(b4[0]) | (uint32(b4[1]) << 8) | (uint32(b4[2]) << 16) | (uint32(b4[3]) << 24))

	// read Apportion
	if _, err := io.ReadAtLeast(wire, b4[:], 4); err != nil {
		return err
	}
	t.Apportion = int32(uint32(b4[0]) | (uint32(b4[1]) << 8) | (uint32(b4[2]) << 16) | (uint32(b4[3]) << 24))

	// read Shard
	if _, err := io.ReadAtLeast(wire, b4[:], 4); err != nil {
		return err
	}
	t.Shard = int32(uint32(b4[0]) | (uint32(b4[1]) << 8) | (uint32(b4[2]) << 16) | (uint32(b4[3]) << 24))

	// read CandidateId
	if _, err := io.ReadAtLeast(wire, b4[:], 4); err != nil {
		return err
	}
	t.CandidateId = int32(uint32(b4[0]) | (uint32(b4[1]) << 8) | (uint32(b4[2]) << 16) | (uint32(b4[3]) << 24))

	// read CandidateCommit
	if _, err := io.ReadAtLeast(wire, b4[:], 4); err != nil {
		return err
	}
	t.CandidateCommit = int32(uint32(b4[0]) | (uint32(b4[1]) << 8) | (uint32(b4[2]) << 16) | (uint32(b4[3]) << 24))

	return nil
}

/***** VoteAndGatherReply *****/

func (t *VoteAndGatherReply) New() fastrpc.Serializable { return new(VoteAndGatherReply) }
func (t *VoteAndGatherReply) BinarySize() (nbytes int, sizeKnown bool) {
	// contains variable-length []LogEntry -> size unknown
	return 0, false
}

type VoteAndGatherReplyCache struct {
	mu    sync.Mutex
	cache []*VoteAndGatherReply
}

func NewVoteAndGatherReplyCache() *VoteAndGatherReplyCache {
	c := &VoteAndGatherReplyCache{}
	c.cache = make([]*VoteAndGatherReply, 0)
	return c
}

func (p *VoteAndGatherReplyCache) Get() *VoteAndGatherReply {
	var t *VoteAndGatherReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &VoteAndGatherReply{}
	}
	return t
}

func (p *VoteAndGatherReplyCache) Put(t *VoteAndGatherReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}

func (t *VoteAndGatherReply) Marshal(wire io.Writer) {
	var b [12]byte
	var bs []byte
	var tmp32 int32

	// write Epoch (4 bytes)
	bs = b[:4]
	tmp32 = t.Epoch
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)

	// write Apportion (4 bytes)
	bs = b[:4]
	tmp32 = t.Apportion
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)

	// write Shard (4 bytes)
	bs = b[:4]
	tmp32 = t.Shard
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)

	// write PeerId (4 bytes)
	bs = b[:4]
	tmp32 = t.PeerId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)

	// write CandidateCommit (4 bytes)
	bs = b[:4]
	tmp32 = t.PeerCommit
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)

	// Marshal Diff ([]LogEntry)
	bs = b[:]
	alen := int64(len(t.Diff))
	if wlen := binary.PutVarint(bs, alen); wlen >= 0 {
		wire.Write(b[:wlen])
	}
	for i := int64(0); i < alen; i++ {
		// 1. Command
		t.Diff[i].Command.Marshal(wire)
		// 2. Epoch (int32)
		var epochBuf [4]byte
		binary.LittleEndian.PutUint32(epochBuf[:], uint32(t.Diff[i].Epoch))
		wire.Write(epochBuf[:])
		// 3. Apportion (int32)
		var apportionBuf [4]byte
		binary.LittleEndian.PutUint32(apportionBuf[:], uint32(t.Diff[i].Apportion))
		wire.Write(apportionBuf[:])
	}

	// write OK (1 byte)
	var b1 [1]byte
	if t.OK {
		b1[0] = byte(1)
	} else {
		b1[0] = byte(0)
	}
	wire.Write(b1[:1])
}

func (t *VoteAndGatherReply) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}

	var b4 [4]byte

	// read Epoch
	if _, err := io.ReadAtLeast(wire, b4[:], 4); err != nil {
		return err
	}
	t.Epoch = int32((uint32(b4[0]) | (uint32(b4[1]) << 8) | (uint32(b4[2]) << 16) | (uint32(b4[3]) << 24)))

	// read Apportion
	if _, err := io.ReadAtLeast(wire, b4[:], 4); err != nil {
		return err
	}
	t.Apportion = int32((uint32(b4[0]) | (uint32(b4[1]) << 8) | (uint32(b4[2]) << 16) | (uint32(b4[3]) << 24)))

	// read Shard
	if _, err := io.ReadAtLeast(wire, b4[:], 4); err != nil {
		return err
	}
	t.Shard = int32((uint32(b4[0]) | (uint32(b4[1]) << 8) | (uint32(b4[2]) << 16) | (uint32(b4[3]) << 24)))

	// read PeerId
	if _, err := io.ReadAtLeast(wire, b4[:], 4); err != nil {
		return err
	}
	t.PeerId = int32((uint32(b4[0]) | (uint32(b4[1]) << 8) | (uint32(b4[2]) << 16) | (uint32(b4[3]) << 24)))

	// read CandidateCommit
	if _, err := io.ReadAtLeast(wire, b4[:], 4); err != nil {
		return err
	}
	t.PeerCommit = int32((uint32(b4[0]) | (uint32(b4[1]) << 8) | (uint32(b4[2]) << 16) | (uint32(b4[3]) << 24)))

	// read Diff ([]LogEntry)
	alen, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Diff = make([]LogEntry, alen)
	for i := int64(0); i < alen; i++ {
		// 1. Command
		if err := t.Diff[i].Command.Unmarshal(wire); err != nil {
			return err
		}
		// 2. Epoch
		var epochBuf [4]byte
		if _, err := io.ReadFull(wire, epochBuf[:]); err != nil {
			return err
		}
		t.Diff[i].Epoch = int32(binary.LittleEndian.Uint32(epochBuf[:]))
		// 3. Apportion
		var apportionBuf [4]byte
		if _, err := io.ReadFull(wire, apportionBuf[:]); err != nil {
			return err
		}
		t.Diff[i].Apportion = int32(binary.LittleEndian.Uint32(apportionBuf[:]))
	}

	// read OK (1 byte)
	var b1 [1]byte
	if _, err := io.ReadAtLeast(wire, b1[:], 1); err != nil {
		return err
	}
	t.OK = (b1[0] != 0)

	return nil
}

/***** AppendEntriesArgs *****/

func (t *AppendEntriesArgs) New() fastrpc.Serializable { return new(AppendEntriesArgs) }
func (t *AppendEntriesArgs) BinarySize() (nbytes int, sizeKnown bool) {
	// contains variable-length []LogEntry -> size unknown
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
	var b [12]byte
	var bs []byte
	var tmp32 int32

	// write Epoch (4 bytes)
	bs = b[:4]
	tmp32 = t.Epoch
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)

	// write Apportion (4 bytes)
	bs = b[:4]
	tmp32 = t.Apportion
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)

	// write CurrentStatus (4 bytes)
	bs = b[:4]
	tmp32 = t.CurrentStatus
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)

	// write CandidateCommit and StartIndex (8 bytes)
	bs = b[:8]
	tmp32 = t.LeaderCommit
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.StartIndex
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	wire.Write(bs)

	// Marshal Diff ([]LogEntry)
	bs = b[:]
	alen := int64(len(t.Diff))
	if wlen := binary.PutVarint(bs, alen); wlen >= 0 {
		wire.Write(b[:wlen])
	}
	for i := int64(0); i < alen; i++ {
		// 1. Command
		t.Diff[i].Command.Marshal(wire)
		// 2. Epoch (int32)
		var epochBuf [4]byte
		binary.LittleEndian.PutUint32(epochBuf[:], uint32(t.Diff[i].Epoch))
		wire.Write(epochBuf[:])
		// 3. Apportion (int32)
		var apportionBuf [4]byte
		binary.LittleEndian.PutUint32(apportionBuf[:], uint32(t.Diff[i].Apportion))
		wire.Write(apportionBuf[:])
	}

	// write LeaderId (4 bytes)
	bs = b[:4]
	tmp32 = t.LeaderId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)

	// write Shard (4 bytes)
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

	var b4 [4]byte
	var b8 [8]byte

	// read Epoch
	if _, err := io.ReadAtLeast(wire, b4[:], 4); err != nil {
		return err
	}
	t.Epoch = int32((uint32(b4[0]) | (uint32(b4[1]) << 8) | (uint32(b4[2]) << 16) | (uint32(b4[3]) << 24)))

	// read Apportion
	if _, err := io.ReadAtLeast(wire, b4[:], 4); err != nil {
		return err
	}
	t.Apportion = int32((uint32(b4[0]) | (uint32(b4[1]) << 8) | (uint32(b4[2]) << 16) | (uint32(b4[3]) << 24)))

	// read CurrentStatus
	if _, err := io.ReadAtLeast(wire, b4[:], 4); err != nil {
		return err
	}
	t.CurrentStatus = int32((uint32(b4[0]) | (uint32(b4[1]) << 8) | (uint32(b4[2]) << 16) | (uint32(b4[3]) << 24)))

	// read CandidateCommit and StartIndex
	if _, err := io.ReadAtLeast(wire, b8[:], 8); err != nil {
		return err
	}
	t.LeaderCommit = int32((uint32(b8[0]) | (uint32(b8[1]) << 8) | (uint32(b8[2]) << 16) | (uint32(b8[3]) << 24)))
	t.StartIndex = int32((uint32(b8[4]) | (uint32(b8[5]) << 8) | (uint32(b8[6]) << 16) | (uint32(b8[7]) << 24)))

	// read Diff ([]LogEntry)
	alen, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Diff = make([]LogEntry, alen)
	for i := int64(0); i < alen; i++ {
		// 1. Command
		if err := t.Diff[i].Command.Unmarshal(wire); err != nil {
			return err
		}
		// 2. Epoch
		var epochBuf [4]byte
		if _, err := io.ReadFull(wire, epochBuf[:]); err != nil {
			return err
		}
		t.Diff[i].Epoch = int32(binary.LittleEndian.Uint32(epochBuf[:]))
		// 3. Apportion
		var apportionBuf [4]byte
		if _, err := io.ReadFull(wire, apportionBuf[:]); err != nil {
			return err
		}
		t.Diff[i].Apportion = int32(binary.LittleEndian.Uint32(apportionBuf[:]))
	}

	// read LeaderId
	if _, err := io.ReadAtLeast(wire, b4[:], 4); err != nil {
		return err
	}
	t.LeaderId = int32((uint32(b4[0]) | (uint32(b4[1]) << 8) | (uint32(b4[2]) << 16) | (uint32(b4[3]) << 24)))

	// read Shard
	if _, err := io.ReadAtLeast(wire, b4[:], 4); err != nil {
		return err
	}
	t.Shard = int32((uint32(b4[0]) | (uint32(b4[1]) << 8) | (uint32(b4[2]) << 16) | (uint32(b4[3]) << 24)))

	return nil
}

/***** AppendEntriesReply *****/

func (t *AppendEntriesReply) New() fastrpc.Serializable { return new(AppendEntriesReply) }
func (t *AppendEntriesReply) BinarySize() (nbytes int, sizeKnown bool) {
	// contains only fixed-size fields -> size known
	return 17, true // 4+4+4+1+4 = 17 bytes
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
	var b [12]byte
	var bs []byte
	var tmp32 int32

	// write Epoch (4 bytes)
	bs = b[:4]
	tmp32 = t.Epoch
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)

	// write Apportion (4 bytes)
	bs = b[:4]
	tmp32 = t.Apportion
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)

	// write CandidateCommit (4 bytes)
	bs = b[:4]
	tmp32 = t.CommitIndex
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)

	// write OK (1 byte)
	var b1 [1]byte
	if t.OK {
		b1[0] = byte(1)
	} else {
		b1[0] = byte(0)
	}
	wire.Write(b1[:1])

	// write FollowerId (4 bytes)
	bs = b[:4]
	tmp32 = t.FollowerId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)

	// write Shard (4 bytes)
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

	var b4 [4]byte

	// read Epoch
	if _, err := io.ReadAtLeast(wire, b4[:], 4); err != nil {
		return err
	}
	t.Epoch = int32(uint32(b4[0]) | uint32(b4[1])<<8 | uint32(b4[2])<<16 | uint32(b4[3])<<24)

	// read Apportion
	if _, err := io.ReadAtLeast(wire, b4[:], 4); err != nil {
		return err
	}
	t.Apportion = int32(uint32(b4[0]) | uint32(b4[1])<<8 | uint32(b4[2])<<16 | uint32(b4[3])<<24)

	// read CandidateCommit
	if _, err := io.ReadAtLeast(wire, b4[:], 4); err != nil {
		return err
	}
	t.CommitIndex = int32(uint32(b4[0]) | uint32(b4[1])<<8 | uint32(b4[2])<<16 | uint32(b4[3])<<24)

	// read OK (1 byte)
	var b1 [1]byte
	if _, err := io.ReadAtLeast(wire, b1[:], 1); err != nil {
		return err
	}
	t.OK = (b1[0] != 0)

	// read FollowerId
	if _, err := io.ReadAtLeast(wire, b4[:], 4); err != nil {
		return err
	}
	t.FollowerId = int32(uint32(b4[0]) | uint32(b4[1])<<8 | uint32(b4[2])<<16 | uint32(b4[3])<<24)

	// read Shard
	if _, err := io.ReadAtLeast(wire, b4[:], 4); err != nil {
		return err
	}
	t.Shard = int32(uint32(b4[0]) | uint32(b4[1])<<8 | uint32(b4[2])<<16 | uint32(b4[3])<<24)

	return nil
}

/***** BalanceArgs *****/

func (t *BalanceArgs) New() fastrpc.Serializable { return new(BalanceArgs) }
func (t *BalanceArgs) BinarySize() (nbytes int, sizeKnown bool) {
	// only fixed-size fields -> size known
	return 8, true // LeaderId (4) + Shard (4)
}

type BalanceArgsCache struct {
	mu    sync.Mutex
	cache []*BalanceArgs
}

func NewBalanceArgsCache() *BalanceArgsCache {
	c := &BalanceArgsCache{}
	c.cache = make([]*BalanceArgs, 0)
	return c
}

func (p *BalanceArgsCache) Get() *BalanceArgs {
	var t *BalanceArgs
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0 : len(p.cache)-1]
	}
	p.mu.Unlock()
	if t == nil {
		t = &BalanceArgs{}
	}
	return t
}

func (p *BalanceArgsCache) Put(t *BalanceArgs) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}

func (t *BalanceArgs) Marshal(wire io.Writer) {
	var b [4]byte
	var bs []byte
	var tmp32 int32

	// write LeaderId (4 bytes)
	bs = b[:4]
	tmp32 = t.LeaderId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)

	// write Sender (4 bytes)
	bs = b[:4]
	tmp32 = t.Sender
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)

	// write ProposalApportion (4 bytes)
	bs = b[:4]
	tmp32 = t.ProposalApportion
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)

	// write Shard (4 bytes)
	bs = b[:4]
	tmp32 = t.Shard
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *BalanceArgs) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}

	var b4 [4]byte

	// read LeaderId
	if _, err := io.ReadAtLeast(wire, b4[:], 4); err != nil {
		return err
	}
	t.LeaderId = int32(uint32(b4[0]) | uint32(b4[1])<<8 | uint32(b4[2])<<16 | uint32(b4[3])<<24)

	// read Sender
	if _, err := io.ReadAtLeast(wire, b4[:], 4); err != nil {
		return err
	}
	t.Sender = int32(uint32(b4[0]) | uint32(b4[1])<<8 | uint32(b4[2])<<16 | uint32(b4[3])<<24)

	// read ProposalApportion
	if _, err := io.ReadAtLeast(wire, b4[:], 4); err != nil {
		return err
	}
	t.ProposalApportion = int32(uint32(b4[0]) | uint32(b4[1])<<8 | uint32(b4[2])<<16 | uint32(b4[3])<<24)

	// read Shard
	if _, err := io.ReadAtLeast(wire, b4[:], 4); err != nil {
		return err
	}
	t.Shard = int32(uint32(b4[0]) | uint32(b4[1])<<8 | uint32(b4[2])<<16 | uint32(b4[3])<<24)

	return nil
}

/***** BalanceReply *****/

func (t *BalanceReply) New() fastrpc.Serializable { return new(BalanceReply) }
func (t *BalanceReply) BinarySize() (nbytes int, sizeKnown bool) {
	// fixed-size fields: Token (1 byte) + Shard (4 bytes)
	return 5, true
}

type BalanceReplyCache struct {
	mu    sync.Mutex
	cache []*BalanceReply
}

func NewBalanceReplyCache() *BalanceReplyCache {
	c := &BalanceReplyCache{}
	c.cache = make([]*BalanceReply, 0)
	return c
}

func (p *BalanceReplyCache) Get() *BalanceReply {
	var t *BalanceReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0 : len(p.cache)-1]
	}
	p.mu.Unlock()
	if t == nil {
		t = &BalanceReply{}
	}
	return t
}

func (p *BalanceReplyCache) Put(t *BalanceReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}

func (t *BalanceReply) Marshal(wire io.Writer) {
	var b [4]byte
	var bs []byte
	var tmp32 int32

	// write Token (1 byte)
	var b1 [1]byte
	if t.Token {
		b1[0] = byte(1)
	} else {
		b1[0] = byte(0)
	}
	wire.Write(b1[:1])

	// write Shard (4 bytes)
	bs = b[:4]
	tmp32 = t.Shard
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *BalanceReply) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}

	// read Token
	var b1 [1]byte
	if _, err := io.ReadAtLeast(wire, b1[:], 1); err != nil {
		return err
	}
	t.Token = (b1[0] != 0)

	// read Shard
	var b4 [4]byte
	if _, err := io.ReadAtLeast(wire, b4[:], 4); err != nil {
		return err
	}
	t.Shard = int32(uint32(b4[0]) | uint32(b4[1])<<8 | uint32(b4[2])<<16 | uint32(b4[3])<<24)

	return nil
}
