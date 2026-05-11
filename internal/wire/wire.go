package wire

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
)

// MaxMessageSize is a safety cap (16 MiB) for a single frame.
const MaxMessageSize = 1 << 24

// WriteFrame writes a length-prefixed JSON payload (big-endian uint32).
func WriteFrame(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(b) > MaxMessageSize {
		return fmt.Errorf("message too large: %d", len(b))
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(b)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

// ReadFrame reads the next length-prefixed JSON object into v.
func ReadFrame(r io.Reader, v any) error {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > MaxMessageSize {
		return fmt.Errorf("message too large: %d", n)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return err
	}
	return json.Unmarshal(buf, v)
}

// ReadLoop calls ReadFrame in a loop until error (EOF, reset).
func ReadLoop(c net.Conn, on func(r io.Reader) error) error {
	for {
		if err := on(c); err != nil {
			return err
		}
	}
}
