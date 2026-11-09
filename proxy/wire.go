package proxy

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// The PostgreSQL wire protocol, version 3. After startup every message is a
// type byte, a 4 byte big endian length (which counts itself but not the
// type byte) and a body. Startup packets have no type byte.

const (
	maxBody       = 1 << 30 // PostgreSQL's own limit is just under 1 GB
	maxStartupLen = 10000   // PostgreSQL rejects longer startup packets

	codeSSLRequest    = 80877103
	codeGSSENCRequest = 80877104
	codeCancelRequest = 80877102
)

var errBadLength = errors.New("isowatch proxy: invalid message length")

// reader reads whole messages. The body it returns is only valid until the
// next call.
type reader struct {
	r   *bufio.Reader
	buf []byte
}

func newReader(r io.Reader) *reader {
	return &reader{r: bufio.NewReaderSize(r, 64<<10)}
}

func (m *reader) body(n int) []byte {
	if cap(m.buf) < n {
		m.buf = make([]byte, n, max(n, 2*cap(m.buf)))
	}
	return m.buf[:n]
}

// next reads one typed message.
func (m *reader) next() (byte, []byte, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(m.r, hdr[:]); err != nil {
		return 0, nil, err
	}
	n := int(binary.BigEndian.Uint32(hdr[1:]))
	if n < 4 || n-4 > maxBody {
		return 0, nil, errBadLength
	}
	b := m.body(n - 4)
	if _, err := io.ReadFull(m.r, b); err != nil {
		return 0, nil, err
	}
	return hdr[0], b, nil
}

// startup reads one untyped startup packet and returns it whole, length
// included, so it can be forwarded byte for byte.
func (m *reader) startup() ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(m.r, hdr[:]); err != nil {
		return nil, err
	}
	n := int(binary.BigEndian.Uint32(hdr[:]))
	if n < 8 || n > maxStartupLen {
		return nil, errBadLength
	}
	b := make([]byte, n)
	copy(b, hdr[:])
	if _, err := io.ReadFull(m.r, b[4:]); err != nil {
		return nil, err
	}
	return b, nil
}

// buffered reports whether a whole message is already waiting in the
// buffer. When it is not, the caller flushes what it wrote before it blocks
// on the next read, so nothing is held back.
func (m *reader) buffered() bool {
	n := m.r.Buffered()
	if n < 5 {
		return false
	}
	hdr, err := m.r.Peek(5)
	if err != nil {
		return false
	}
	return n >= 1+int(binary.BigEndian.Uint32(hdr[1:]))
}

// writeMsg writes one typed message.
func writeMsg(w *bufio.Writer, typ byte, body []byte) error {
	var hdr [5]byte
	hdr[0] = typ
	binary.BigEndian.PutUint32(hdr[1:], uint32(len(body)+4))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(body)
	return err
}

// cstring splits a NUL terminated string off the front of b.
func cstring(b []byte) (string, []byte, error) {
	i := bytes.IndexByte(b, 0)
	if i < 0 {
		return "", nil, errors.New("isowatch proxy: missing string terminator")
	}
	return string(b[:i]), b[i+1:], nil
}

// startupParams decodes the key/value pairs of a StartupMessage packet.
func startupParams(pkt []byte) map[string]string {
	out := map[string]string{}
	b := pkt[8:]
	for len(b) > 1 {
		k, rest, err := cstring(b)
		if err != nil || k == "" {
			break
		}
		v, rest, err := cstring(rest)
		if err != nil {
			break
		}
		out[k] = v
		b = rest
	}
	return out
}

// errorResponse builds an ErrorResponse body.
func errorResponse(severity, code, msg string) []byte {
	var b []byte
	b = append(b, 'S')
	b = append(b, severity...)
	b = append(b, 0, 'V')
	b = append(b, severity...)
	b = append(b, 0, 'C')
	b = append(b, code...)
	b = append(b, 0, 'M')
	b = append(b, msg...)
	b = append(b, 0, 0)
	return b
}

// errorFields reads the severity and SQLSTATE of an ErrorResponse body.
func errorFields(body []byte) (severity, code string) {
	for len(body) > 1 {
		f := body[0]
		v, rest, err := cstring(body[1:])
		if err != nil {
			break
		}
		switch f {
		case 'V':
			severity = v
		case 'S':
			if severity == "" {
				severity = v
			}
		case 'C':
			code = v
		}
		body = rest
	}
	return severity, code
}

// splitRow finds the byte offset where column `keep` of a DataRow body
// starts and returns the values of the columns from there on. The values
// are copied, because the body is reused for the next message.
func splitRow(body []byte, keep int) (offset int, tail [][]byte, err error) {
	if len(body) < 2 {
		return 0, nil, errors.New("isowatch proxy: short DataRow")
	}
	n := int(binary.BigEndian.Uint16(body))
	if keep > n {
		return 0, nil, fmt.Errorf("isowatch proxy: DataRow has %d columns, expected at least %d", n, keep)
	}
	pos := 2
	for i := 0; i < n; i++ {
		if i == keep {
			offset = pos
			tail = make([][]byte, 0, n-keep)
		}
		if len(body) < pos+4 {
			return 0, nil, errors.New("isowatch proxy: short DataRow")
		}
		l := int(int32(binary.BigEndian.Uint32(body[pos:])))
		pos += 4
		if l < -1 {
			return 0, nil, errors.New("isowatch proxy: bad DataRow value length")
		}
		var v []byte
		if l >= 0 {
			if len(body) < pos+l {
				return 0, nil, errors.New("isowatch proxy: short DataRow")
			}
			if i >= keep {
				v = append(make([]byte, 0, l), body[pos:pos+l]...)
			}
			pos += l
		}
		if i >= keep {
			tail = append(tail, v)
		}
	}
	if keep == n {
		offset = pos
	}
	if pos != len(body) {
		return 0, nil, errors.New("isowatch proxy: DataRow has trailing bytes")
	}
	return offset, tail, nil
}
