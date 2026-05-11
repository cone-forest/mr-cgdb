package netx

import (
	"fmt"
	"net"
	"time"
)

// DialTCP dials with small retries.
func DialTCP(addr string) (net.Conn, error) {
	var last error
	for i := 0; i < 20; i++ {
		c, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err == nil {
			return c, nil
		}
		last = err
		time.Sleep(500 * time.Millisecond)
	}
	return nil, fmt.Errorf("dial %s: %w", addr, last)
}
