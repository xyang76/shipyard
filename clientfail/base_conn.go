package clientfail

import (
	"Mix/config"
	"Mix/dlog"
	"Mix/genericsmr"
	"Mix/genericsmrproto"
	"Mix/state"
	"bufio"
	"fmt"
	"net"
	"sync"
	"time"
)

const Max_Pending = 10000

type BaseConn struct {
	addr          string
	conn          net.Conn
	reader        *bufio.Reader
	writer        *bufio.Writer
	writeCounter  int
	writerMu      sync.Mutex // protects writer + flush
	sendCh        chan *genericsmrproto.Propose
	skipCounter   int
	finishCounter int
	notify        *FinishNotify
	connecting    bool
	reconnecting  bool
	rid           int
	maxPending    int64
	autoReconnect bool
	mu            sync.Mutex
}

func NewBaseConn(addr string, rid int, notify *FinishNotify) *BaseConn {
	c := &BaseConn{
		addr:          addr,
		notify:        notify,
		rid:           rid,
		maxPending:    Max_Pending,
		sendCh:        make(chan *genericsmrproto.Propose, config.CHAN_BUFFER_SIZE),
		autoReconnect: true,
	}
	return c
}

func (c *BaseConn) InitConnection() {
	c.Connect()
	go c.SendLoop()
	go c.PeriodicalFlush()
}

func (c *BaseConn) PeriodicalFlush() {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		c.mu.Lock()
		conn := c.conn
		writer := c.writer
		writes := c.writeCounter
		finishes := c.finishCounter
		c.mu.Unlock()

		if conn == nil || writer == nil || writes == finishes {
			continue
		}

		c.writerMu.Lock()
		err := genericsmr.WriteWithTimeout(conn, func() error {
			return writer.Flush()
		})
		c.writerMu.Unlock()

		if err != nil {
			c.notify.notifyConnectFail(err)
			c.Disconnect()
		}
	}
}

func (c *BaseConn) SendLoop() {
	for {
		item := <-c.sendCh

		for {
			c.mu.Lock()
			inFlight := c.writeCounter - c.finishCounter
			conn := c.conn
			writer := c.writer
			c.mu.Unlock()

			if conn != nil && writer != nil && inFlight < int(c.maxPending) {
				break
			}

			time.Sleep(50 * time.Microsecond)
		}

		c.writerMu.Lock()
		err := genericsmr.WriteWithTimeout(c.conn, func() error {
			if err := c.writer.WriteByte(genericsmrproto.PROPOSE); err != nil {
				return err
			}
			item.Marshal(c.writer)
			c.writeCounter++
			if c.writeCounter%100 == 0 {
				if err := c.writer.Flush(); err != nil {
					return err
				}
			}
			return nil
		})
		c.writerMu.Unlock()

		if err != nil {
			c.notify.notifyConnectFail(err)
			c.notify.notifyCommandFail(item, err)
			c.Disconnect()
			continue
		}

		//c.mu.Lock()
		//c.writeCounter++
		//c.mu.Unlock()
	}
}

func (c *BaseConn) Connect() {
	c.mu.Lock()
	if c.connecting {
		c.mu.Unlock()
		return
	}
	c.connecting = true
	c.mu.Unlock() // unlock before dialing to avoid blocking other ops

	conn, err := net.DialTimeout("tcp", c.addr, genericsmr.CONN_TIMEOUT)
	if err != nil {
		dlog.Info("dial addr %v failed: %v", c.addr, err)
		c.mu.Lock()
		c.connecting = false // reset so future Connect() can retry
		c.mu.Unlock()
		return
	}
	dlog.Info("connected addr %v", c.addr)

	c.mu.Lock()
	c.conn = conn
	c.reader = bufio.NewReader(conn)
	c.writer = bufio.NewWriter(conn)
	c.connecting = false
	c.mu.Unlock()

	go c.replyReader()
}

func (c *BaseConn) reconnectLoop() {
	defer func() {
		c.mu.Lock()
		c.reconnecting = false
		c.mu.Unlock()
	}()

	for {
		time.Sleep(time.Second)

		c.mu.Lock()
		if c.conn != nil {
			c.mu.Unlock()
			return
		}
		c.mu.Unlock()

		c.Connect()

		c.mu.Lock()
		ok := c.conn != nil
		c.mu.Unlock()

		if ok {
			return
		}
	}
}

func (c *BaseConn) SetConnection(conn net.Conn) {
	c.mu.Lock()
	c.connecting = false
	c.conn = conn
	c.reader = bufio.NewReader(conn)
	c.writer = bufio.NewWriter(conn)
	c.mu.Unlock()

	go c.replyReader()
}

func (c *BaseConn) Disconnect() {
	c.mu.Lock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
		c.reader = nil
		c.writer = nil
		c.writeCounter = 0
		c.skipCounter = 0
		c.finishCounter = 0
	}
	if !c.reconnecting && c.autoReconnect {
		c.reconnecting = true
		go c.reconnectLoop()
	}
	c.connecting = false
	c.mu.Unlock()
}

func (c *BaseConn) SendPropose(args *genericsmrproto.Propose) {
	select {
	case c.sendCh <- args:
		// successfully enqueued
	default:
		// queue full → skip
		c.mu.Lock()
		c.skipCounter++
		c.mu.Unlock()
		c.notify.notifyCommandSkip(args.CommandId)
	}
}

func (c *BaseConn) replyReader() {

	for {
		c.mu.Lock()
		conn := c.conn
		reader := c.reader
		c.mu.Unlock()

		if conn == nil || reader == nil {
			return
		}

		reply := new(genericsmrproto.ProposeReplyTS)
		err := genericsmr.ReadWithTimeout(conn, func() error {
			return reply.Unmarshal(reader)
		})

		if err != nil {
			// Timeout ≠ leader failed
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			c.notify.notifyConnectFail(err)
			c.Disconnect()
			return
		}

		c.notify.notifyCommandFinish(reply)
		c.mu.Lock()
		c.finishCounter++
		c.mu.Unlock()

		if reply.CommandId != config.IdentifyLeader &&
			reply.Value == state.NOTLeader &&
			reply.OK == config.FALSE {
			c.notify.notifyInvalidLeader(c.rid, reply, fmt.Errorf("invalid leader"))
			continue
		}
	}
}
