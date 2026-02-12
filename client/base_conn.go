package client

import (
	"bufio"
	"net"
)

type BaseConn struct {
	conn         net.Conn
	reader       *bufio.Reader
	writer       *bufio.Writer
	writeCounter int
}
