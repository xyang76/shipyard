package genericsmr

import (
	"net"
	"time"
)

const WRITE_TIMEOUT = 1 * time.Second
const READ_TIMEOUT = 10 * time.Second
const CONN_TIMEOUT = 3 * time.Second

type Fail int

const (
	OnRead Fail = iota
	OnWrite
	OnConnect
)

func WriteWithTimeout(
	conn net.Conn,
	fn func() error,
) error {
	if err := conn.SetWriteDeadline(time.Now().Add(WRITE_TIMEOUT)); err != nil {
		return err
	}
	defer conn.SetWriteDeadline(time.Time{})

	return fn()
}

func ReadWithTimeout(
	conn net.Conn,
	fn func() error,
) error {
	if err := conn.SetReadDeadline(time.Now().Add(READ_TIMEOUT)); err != nil {
		return err
	}
	defer conn.SetReadDeadline(time.Time{})

	return fn()
}
