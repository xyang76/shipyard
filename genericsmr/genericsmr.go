package genericsmr

import (
	"Mix/config"
	"Mix/dlog"
	"Mix/fastrpc"
	"Mix/genericsmrproto"
	"Mix/rdtsc"
	"Mix/state"
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

const CHAN_BUFFER_SIZE = 200000

type RPCPair struct {
	Obj  fastrpc.Serializable
	Chan chan fastrpc.Serializable
}

type Propose struct {
	*genericsmrproto.Propose
	Reply *bufio.Writer
}

type Beacon struct {
	Rid       int32
	Timestamp uint64
}

type Replica struct {
	N            int        // total number of replicas
	Id           int32      // the ID of the current replica
	PeerAddrList []string   // array with the IP:port address of every replica
	Peers        []net.Conn // cache of connections to all other replicas
	PeerReaders  []*bufio.Reader
	PeerWriters  []*bufio.Writer
	peerWriteMu  []sync.Mutex //Added by Shipyard's author, to avoid concurrent writes
	peerConnMu   []sync.Mutex
	Alive        []bool // connection status
	Connecting   []bool
	Listener     net.Listener
	ClientListen net.Listener // This is a separate listener for client only(For failure reconnection)

	State *state.State

	ProposeChan chan *Propose // channel for client proposals
	BeaconChan  chan *Beacon  // channel for beacons from peer replicas

	Shutdown bool

	Thrifty bool // send only as many messages as strictly required?
	Exec    bool // execute commands?
	Dreply  bool // reply to client after command has been executed?
	Beacon  bool // send beacons to detect how fast are the other replicas?

	Durable     bool     // log to a stable store?
	StableStore *os.File // file support for the persistent log

	PreferredPeerOrder []int32 // replicas in the preferred order of communication

	rpcTable map[uint8]*RPCPair
	rpcCode  uint8

	Ewma []float64

	OnClientConnect chan bool
}

func NewReplica(id int, peerAddrList []string, thrifty bool, exec bool, dreply bool) *Replica {
	r := &Replica{
		len(peerAddrList),
		int32(id),
		peerAddrList,
		make([]net.Conn, len(peerAddrList)),
		make([]*bufio.Reader, len(peerAddrList)),
		make([]*bufio.Writer, len(peerAddrList)),
		make([]sync.Mutex, len(peerAddrList)),
		make([]sync.Mutex, len(peerAddrList)),
		make([]bool, len(peerAddrList)),
		make([]bool, len(peerAddrList)),
		nil,
		nil,
		state.InitState(),
		make(chan *Propose, CHAN_BUFFER_SIZE),
		make(chan *Beacon, CHAN_BUFFER_SIZE),
		false,
		thrifty,
		exec,
		dreply,
		false,
		false,
		nil,
		make([]int32, len(peerAddrList)),
		make(map[uint8]*RPCPair),
		genericsmrproto.GENERIC_SMR_BEACON_REPLY + 1,
		make([]float64, len(peerAddrList)),
		make(chan bool, 100)}

	var err error

	if r.StableStore, err = os.Create(fmt.Sprintf("stable-store-replica%d", r.Id)); err != nil {
		log.Fatal(err)
	}

	for i := 0; i < r.N; i++ {
		r.PreferredPeerOrder[i] = int32((int(r.Id) + 1 + i) % r.N)
		r.Ewma[i] = 0.0
	}

	return r
}

/* Client API */

func (r *Replica) Ping(args *genericsmrproto.PingArgs, reply *genericsmrproto.PingReply) error {
	return nil
}

func (r *Replica) BeTheLeader(args *genericsmrproto.BeTheLeaderArgs, reply *genericsmrproto.BeTheLeaderReply) error {
	return nil
}

/* ============= */

func (r *Replica) ConnectToPeers() {
	var b [4]byte
	bs := b[:4]
	done := make(chan bool)
	if *config.Recovered == 1 {
		dlog.Info("replica %v is recovering, connect to peers ...", r.Id)
	}

	go r.waitForPeerConnections(done)

	//connect to peers
	for i := 0; i < int(r.Id); i++ {
		for done := false; !done; {
			if conn, err := net.Dial("tcp", r.PeerAddrList[i]); err == nil {
				r.Peers[i] = conn
				done = true
			} else {
				time.Sleep(1e9)
			}
		}
		binary.LittleEndian.PutUint32(bs, uint32(r.Id))
		if _, err := r.Peers[i].Write(bs); err != nil {
			fmt.Println("Write id error:", err)
			continue
		}
		r.Alive[i] = true
		r.Connecting[i] = false
		r.PeerReaders[i] = bufio.NewReader(r.Peers[i])
		r.PeerWriters[i] = bufio.NewWriter(r.Peers[i])
		if *config.Recovered == 1 {
			dlog.Info("replica %v is recovering, connected to peers %v [addr:%v]...", r.Id, i, r.PeerAddrList[i])
		}
	}
	<-done
	log.Printf("Replica id: %d. Done connecting to peers\n", r.Id)

	for rid, reader := range r.PeerReaders {
		if int32(rid) == r.Id {
			continue
		}
		if config.Fail_Prone {
			go r.replicaListenerFailProne(int32(rid), reader)
		} else {
			go r.replicaListener(rid, reader)
		}
	}
}

func (r *Replica) ConnectToPeersNoListeners() {
	var b [4]byte
	bs := b[:4]
	done := make(chan bool)

	go r.waitForPeerConnections(done)

	//connect to peers
	for i := 0; i < int(r.Id); i++ {
		for done := false; !done; {
			if conn, err := net.Dial("tcp", r.PeerAddrList[i]); err == nil {
				r.Peers[i] = conn
				done = true
			} else {
				time.Sleep(1e9)
			}
		}
		binary.LittleEndian.PutUint32(bs, uint32(r.Id))
		if _, err := r.Peers[i].Write(bs); err != nil {
			fmt.Println("Write id error:", err)
			continue
		}
		r.Alive[i] = true
		r.PeerReaders[i] = bufio.NewReader(r.Peers[i])
		r.PeerWriters[i] = bufio.NewWriter(r.Peers[i])
	}
	<-done
	log.Printf("Replica id: %d. Done connecting to peers\n", r.Id)
}

/* Peer (replica) connections dispatcher */
func (r *Replica) waitForPeerConnections(done chan bool) {
	var b [4]byte
	bs := b[:4]

	r.Listener, _ = net.Listen("tcp", r.PeerAddrList[r.Id])
	for i := r.Id + 1; i < int32(r.N); i++ {
		conn, err := r.Listener.Accept()
		if err != nil {
			fmt.Println("Accept error:", err)
			continue
		}
		if _, err := io.ReadFull(conn, bs); err != nil {
			fmt.Println("Connection establish error:", err)
			continue
		}
		id := int32(binary.LittleEndian.Uint32(bs))
		r.Peers[id] = conn
		r.PeerReaders[id] = bufio.NewReader(conn)
		r.PeerWriters[id] = bufio.NewWriter(conn)
		r.Alive[id] = true
		r.Connecting[id] = false
	}

	done <- true
}

/* Client connections dispatcher */
func (r *Replica) WaitForClientConnections() {
	if *config.SeperateClientPort {
		addr := r.PeerAddrList[r.Id]
		// Add 2000 to get client port
		host, portStr, _ := net.SplitHostPort(addr)
		port, _ := strconv.Atoi(portStr)
		clientPort := port + 2000
		clientAddr := fmt.Sprintf("%s:%d", host, clientPort)

		var err error
		r.ClientListen, err = net.Listen("tcp", clientAddr)
		if err != nil {
			log.Fatalf("Failed to listen on client port: %v", err)
		}
		log.Printf("Replica %d listening for clients on %s\n", r.Id, clientAddr)

		for !r.Shutdown {
			conn, err := r.ClientListen.Accept()
			if err != nil {
				log.Println("Accept error:", err)
				continue
			}
			go r.clientListener(conn)
			r.OnClientConnect <- true
		}
		return
	}

	for !r.Shutdown {
		conn, err := r.Listener.Accept()
		if err != nil {
			log.Println("Accept error:", err)
			continue
		}
		go r.clientListener(conn)

		r.OnClientConnect <- true
	}
}

func (r *Replica) replicaListenerFailProne(rid int32, reader *bufio.Reader) {
	var msgType uint8
	var gbeacon genericsmrproto.Beacon
	var gbeaconReply genericsmrproto.BeaconReply

	conn := r.Peers[rid] // get the connection for this replica
	if conn == nil {
		return
	}

	for !r.Shutdown {

		// Read message type with timeout
		err := ReadWithTimeout(conn, func() error {
			var errInner error
			if msgType, errInner = reader.ReadByte(); errInner != nil {
				return errInner
			}
			return nil
		})
		if err != nil {
			// timeout is NOT a failure
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			// real network error: call failure handler
			r.failHandler(rid, err, OnRead)
			return
		}

		switch uint8(msgType) {

		case genericsmrproto.GENERIC_SMR_BEACON:
			err = ReadWithTimeout(conn, func() error {
				return gbeacon.Unmarshal(reader)
			})
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					continue
				}
				log.Printf("Network error unmarshaling beacon from replica %d: %v", rid, err)
				r.failHandler(rid, err, OnRead)
				return
			}
			beacon := &Beacon{int32(rid), gbeacon.Timestamp}
			r.BeaconChan <- beacon

		case genericsmrproto.GENERIC_SMR_BEACON_REPLY:
			err = ReadWithTimeout(conn, func() error {
				return gbeaconReply.Unmarshal(reader)
			})
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					continue
				}
				log.Printf("Network error unmarshaling beacon reply from replica %d: %v", rid, err)
				r.failHandler(rid, err, OnRead)
				return
			}
			// normal processing
			r.Ewma[rid] = 0.99*r.Ewma[rid] + 0.01*float64(rdtsc.Cputicks()-gbeaconReply.Timestamp)
			log.Println(r.Ewma)

		default:
			if rpair, present := r.rpcTable[msgType]; present {
				obj := rpair.Obj.New()
				err = ReadWithTimeout(conn, func() error {
					return obj.Unmarshal(reader)
				})
				if err != nil {
					if ne, ok := err.(net.Error); ok && ne.Timeout() {
						continue
					}
					log.Printf("Network error unmarshaling RPC object from replica %d: %v", rid, err)
					r.failHandler(rid, err, OnRead)
					return
				}
				rpair.Chan <- obj
			} else {
				log.Println("Error: received unknown message type")
			}
		}
	}
}

func (r *Replica) replicaListener(rid int, reader *bufio.Reader) {
	var msgType uint8
	var err error = nil
	var gbeacon genericsmrproto.Beacon
	var gbeaconReply genericsmrproto.BeaconReply

	for err == nil && !r.Shutdown {

		if msgType, err = reader.ReadByte(); err != nil {
			break
		}

		switch uint8(msgType) {

		case genericsmrproto.GENERIC_SMR_BEACON:
			if err = gbeacon.Unmarshal(reader); err != nil {
				break
			}
			beacon := &Beacon{int32(rid), gbeacon.Timestamp}
			r.BeaconChan <- beacon
			break

		case genericsmrproto.GENERIC_SMR_BEACON_REPLY:
			if err = gbeaconReply.Unmarshal(reader); err != nil {
				break
			}
			//TODO: UPDATE STUFF
			r.Ewma[rid] = 0.99*r.Ewma[rid] + 0.01*float64(rdtsc.Cputicks()-gbeaconReply.Timestamp)
			log.Println(r.Ewma)
			break

		default:
			if rpair, present := r.rpcTable[msgType]; present {
				obj := rpair.Obj.New()
				if err = obj.Unmarshal(reader); err != nil {
					break
				}
				rpair.Chan <- obj
			} else {
				log.Println("Error: received unknown message type")
			}
		}
	}
}

func (r *Replica) clientListener(conn net.Conn) {
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	var msgType byte //:= make([]byte, 1)
	var err error
	for !r.Shutdown && err == nil {

		if msgType, err = reader.ReadByte(); err != nil {
			break
		}

		switch uint8(msgType) {

		case genericsmrproto.PROPOSE:
			prop := new(genericsmrproto.Propose)
			if err = prop.Unmarshal(reader); err != nil {
				break
			}
			r.ProposeChan <- &Propose{prop, writer}
			break

		case genericsmrproto.READ:
			read := new(genericsmrproto.Read)
			if err = read.Unmarshal(reader); err != nil {
				break
			}
			//r.ReadChan <- read
			break

		case genericsmrproto.PROPOSE_AND_READ:
			pr := new(genericsmrproto.ProposeAndRead)
			if err = pr.Unmarshal(reader); err != nil {
				break
			}
			//r.ProposeAndReadChan <- pr
			break
		}
	}
	if err != nil && err != io.EOF {
		log.Println("Error when reading from client connection:", err)
	}
}

func (r *Replica) RegisterRPC(msgObj fastrpc.Serializable, notify chan fastrpc.Serializable) uint8 {
	code := r.rpcCode
	r.rpcCode++
	r.rpcTable[code] = &RPCPair{msgObj, notify}
	return code
}

func (r *Replica) SendMsg(peerId int32, code uint8, msg fastrpc.Serializable) {
	if config.Fail_Prone {
		r.SendMsgFailProne(peerId, code, msg)
	} else {
		r.SendMsgNoFail(peerId, code, msg)
	}
}

func (r *Replica) SendMsgFailProne(peerId int32, code uint8, msg fastrpc.Serializable) {
	r.peerWriteMu[peerId].Lock()
	defer r.peerWriteMu[peerId].Unlock()

	conn := r.Peers[peerId]
	w := r.PeerWriters[peerId]
	if conn == nil || w == nil || !r.Alive[peerId] {
		dlog.Info("%d->%d connection is lost, [%v, %v, %v]", r.Id, peerId, conn == nil, w == nil, !r.Alive[peerId])
		return
	}

	err := WriteWithTimeout(conn, func() error {
		if err := w.WriteByte(code); err != nil {
			return err
		}
		msg.Marshal(w)
		return w.Flush()
	})

	if err != nil {
		dlog.Info("%d->%d connection is err %v", r.Id, peerId, err)
		r.failHandler(peerId, err, OnWrite)
	}
}

func (r *Replica) SendMsgNoFail(peerId int32, code uint8, msg fastrpc.Serializable) {
	r.peerWriteMu[peerId].Lock()
	defer r.peerWriteMu[peerId].Unlock()

	w := r.PeerWriters[peerId]
	w.WriteByte(code)
	msg.Marshal(w)
	w.Flush()
}

func (r *Replica) SendMsgNoFlush(peerId int32, code uint8, msg fastrpc.Serializable) {
	if !config.Fail_Prone {
		w := r.PeerWriters[peerId]
		w.WriteByte(code)
		msg.Marshal(w)
	} else {
		conn := r.Peers[peerId]
		w := r.PeerWriters[peerId]
		if conn == nil || w == nil {
			dlog.Print("%d->%d connection is lost, [%v, %v, %v]", r.Id, peerId, conn == nil, w == nil, !r.Alive[peerId])
			return
		}
		err := WriteWithTimeout(conn, func() error {
			if err := w.WriteByte(code); err != nil {
				return err
			}
			msg.Marshal(w)
			return nil
		})

		if err != nil {
			r.failHandler(peerId, err, OnWrite)
		}
	}
}

func (r *Replica) SendBeacon(peerId int32) {
	if !config.Fail_Prone {
		w := r.PeerWriters[peerId]
		w.WriteByte(genericsmrproto.GENERIC_SMR_BEACON)
		beacon := &genericsmrproto.Beacon{rdtsc.Cputicks()}
		beacon.Marshal(w)
		w.Flush()
	} else {
		conn := r.Peers[peerId]
		w := r.PeerWriters[peerId]
		if conn == nil || w == nil {
			dlog.Print("%d->%d connection is lost, [%v, %v, %v]", r.Id, peerId, conn == nil, w == nil, !r.Alive[peerId])
			return
		}
		err := WriteWithTimeout(conn, func() error {
			w.WriteByte(genericsmrproto.GENERIC_SMR_BEACON)
			beacon := &genericsmrproto.Beacon{rdtsc.Cputicks()}
			beacon.Marshal(w)
			return w.Flush()
		})

		if err != nil {
			r.failHandler(peerId, err, OnWrite)
		}
	}
}

func (r *Replica) ReplyBeacon(beacon *Beacon) {
	w := r.PeerWriters[beacon.Rid]
	w.WriteByte(genericsmrproto.GENERIC_SMR_BEACON_REPLY)
	rb := &genericsmrproto.BeaconReply{beacon.Timestamp}
	rb.Marshal(w)
	w.Flush()
}

func (r *Replica) ReplyPropose(reply *genericsmrproto.ProposeReply, w *bufio.Writer) {
	//r.clientMutex.Lock()
	//defer r.clientMutex.Unlock()
	//w.WriteByte(genericsmrproto.PROPOSE_REPLY)
	reply.Marshal(w)
	w.Flush()
}

func (r *Replica) ReplyProposeTS(reply *genericsmrproto.ProposeReplyTS, w *bufio.Writer) {
	//r.clientMutex.Lock()
	//defer r.clientMutex.Unlock()
	//w.WriteByte(genericsmrproto.PROPOSE_REPLY)
	reply.Marshal(w)
	w.Flush()
}

// updates the preferred order in which to communicate with peers according to a preferred quorum
func (r *Replica) UpdatePreferredPeerOrder(quorum []int32) {
	aux := make([]int32, r.N)
	i := 0
	for _, p := range quorum {
		if p == r.Id {
			continue
		}
		aux[i] = p
		i++
	}

	for _, p := range r.PreferredPeerOrder {
		found := false
		for j := 0; j < i; j++ {
			if aux[j] == p {
				found = true
				break
			}
		}
		if !found {
			aux[i] = p
			i++
		}
	}

	r.PreferredPeerOrder = aux
}

func (r *Replica) failHandler(rid int32, err error, ftype Fail) {
	if r.Shutdown {
		return
	}
	if !r.Alive[rid] {
		return
	}
	dlog.Info("%d: lost connection to peer %d, type is %v, err is %v", r.Id, rid, ftype, err)

	r.Alive[rid] = false

	if r.Peers[rid] != nil {
		r.Peers[rid].Close()
	}

	if r.Id > rid {
		go r.reconnectPeer(rid)
	} else {
		go r.waitForPeerReconnect(rid)
	}
}

func (r *Replica) reconnectPeer(peerId int32) {
	for !r.Shutdown {
		r.peerConnMu[peerId].Lock()
		if r.Connecting[peerId] {
			r.peerConnMu[peerId].Unlock()
			return
		}
		r.Connecting[peerId] = true
		r.peerConnMu[peerId].Unlock()

		dlog.Print("%d: attempting reconnect to peer %d", r.Id, peerId)

		conn, err := net.Dial("tcp", r.PeerAddrList[peerId])
		if err != nil {
			r.peerConnMu[peerId].Lock()
			r.Connecting[peerId] = false
			r.peerConnMu[peerId].Unlock()
			time.Sleep(time.Second)
			continue
		}

		// handshake
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], uint32(r.Id))
		if _, err = conn.Write(b[:]); err != nil {
			conn.Close()
			r.peerConnMu[peerId].Lock()
			r.Connecting[peerId] = false
			r.peerConnMu[peerId].Unlock()
			time.Sleep(config.RecoveryInterval * time.Second)
			continue
		}

		// swap connection
		r.peerWriteMu[peerId].Lock()
		old := r.Peers[peerId]
		r.Peers[peerId] = conn
		r.PeerReaders[peerId] = bufio.NewReader(conn)
		r.PeerWriters[peerId] = bufio.NewWriter(conn)
		r.Alive[peerId] = true
		r.peerWriteMu[peerId].Unlock()

		r.peerConnMu[peerId].Lock()
		r.Connecting[peerId] = false
		r.peerConnMu[peerId].Unlock()

		if old != nil {
			old.Close()
		}

		dlog.Info("%d: successfully reconnected to peer %d", r.Id, peerId)

		if config.Fail_Prone {
			go r.replicaListenerFailProne(peerId, r.PeerReaders[peerId])
		} else {
			go r.replicaListener(int(peerId), r.PeerReaders[peerId])
		}
		return
	}
}

func (r *Replica) waitForPeerReconnect(peerId int32) {
	if r.Shutdown {
		return
	}

	for !r.Shutdown {
		conn, err := r.Listener.Accept()
		if err != nil {
			fmt.Println("Accept error during reconnect:", err)
			continue
		}

		// read handshake (peer ID)
		var b [4]byte
		if _, err := io.ReadFull(conn, b[:]); err != nil {
			conn.Close()
			fmt.Println("Handshake error during reconnect:", err)
			continue
		}

		id := int32(binary.LittleEndian.Uint32(b[:]))

		if id != peerId {
			// not the peer we are waiting for — close it
			conn.Close()
			continue
		}

		// this is the correct peer
		r.peerWriteMu[id].Lock()
		old := r.Peers[id]
		r.Peers[id] = conn
		r.PeerReaders[id] = bufio.NewReader(conn)
		r.PeerWriters[id] = bufio.NewWriter(conn)
		r.Alive[id] = true
		r.Connecting[id] = false
		r.peerWriteMu[id].Unlock()

		if old != nil {
			old.Close()
		}

		// start listener goroutine
		if config.Fail_Prone {
			go r.replicaListenerFailProne(id, r.PeerReaders[id])
		} else {
			go r.replicaListener(int(id), r.PeerReaders[id])
		}

		dlog.Info("%d: successfully reconnected from peer %d", r.Id, id)
		return
	}
}
