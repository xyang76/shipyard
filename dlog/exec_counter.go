package dlog

import (
	"fmt"
	"strconv"
	"sync"
)

// FLogger is a struct with an embedded logger
type ExeCounter struct {
	indexes map[string]int
	index   int
	mu      sync.Mutex
	flog    *FileLogger
}

func NewExeCounter() *ExeCounter {
	flog, _ := NewLogger("log")
	return &ExeCounter{
		indexes: make(map[string]int),
		index:   1,
		flog:    flog,
	}
}

//func (c *ExeCounter) Inc(message string, typ string, args ...interface{}) {
//	msg := fmt.Sprintf(message, args...)
//	c.Write(msg, typ)
//}

func (c *ExeCounter) Inc(v string, num int, message string, args ...interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, exist := c.indexes[v]
	if !exist {
		c.indexes[v] = 0
	}
	c.indexes[v]++
	info := fmt.Sprintf("[%v] (%v)::->", v, c.indexes[v])
	if c.indexes[v]%num == 0 {
		Info(info+message+"\n", args...)
	}
}

func (c *ExeCounter) Count(v string, message string, args ...interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, exist := c.indexes[v]
	if !exist {
		c.indexes[v] = 0
	}
	c.indexes[v]++
	info := fmt.Sprintf("[%v] (%v)::->", v, c.indexes[v])
	if c.indexes[v]%30 == 0 {
		Info(info+message+"\n", args...)
	}
}

func (c *ExeCounter) Write(message string, typ string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.flog.Append(message + " -- executed: " + strconv.Itoa(c.index))
	c.index++
}

func (c *ExeCounter) Valid(session string, id string, s string, args ...interface{}) {
	c.Count(session, id+" "+s, args...)
}

// FLogInstance is a global "constant" instance of FLogger
var EC = NewExeCounter()
