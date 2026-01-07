package genericsmr

import (
	"net"
	"time"
)

const CONN_TIMEOUT = 20 * time.Second

func WriteWithTimeout(
	conn net.Conn,
	fn func() error,
) error {
	if err := conn.SetWriteDeadline(time.Now().Add(CONN_TIMEOUT)); err != nil {
		return err
	}
	defer conn.SetWriteDeadline(time.Time{})

	return fn()
}

func ReadWithTimeout(
	conn net.Conn,
	fn func() error,
) error {
	if err := conn.SetReadDeadline(time.Now().Add(CONN_TIMEOUT)); err != nil {
		return err
	}
	defer conn.SetReadDeadline(time.Time{})

	return fn()
}
