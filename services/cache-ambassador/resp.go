package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
)

// resp.go implements the small RESP2 (REdis Serialization Protocol) subset the
// ambassador needs: a request reader that frames client commands, a reply
// reader that captures a backend's reply verbatim (so it can be relayed
// byte-for-byte), and helpers to (re)encode commands and simple replies.
//
// Only RESP2 reply types are handled (+ simple, - error, : integer, $ bulk,
// * array). That is sufficient because the ambassador forces clients down to
// RESP2 (it rejects HELLO), and a stock Redis backend speaks RESP2 unless a
// client negotiates RESP3 - which no client can do through this proxy.

var errProtocol = fmt.Errorf("resp: protocol error")

// readCommand reads exactly one client command and returns its arguments.
// It understands the RESP multibulk form (`*N` array of `$len` bulk strings),
// which is what real clients (go-redis, redis-cli in non-interactive mode) send,
// and also tolerates a plain inline command line as a convenience.
//
// bufio + io.ReadFull make partial socket reads transparent: a call blocks
// until a whole command is available, and pipelined commands are returned one
// per call across successive invocations.
func readCommand(r *bufio.Reader) ([][]byte, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	if prefix != '*' {
		// Inline command, e.g. "PING\r\n". Read the rest of the line and split.
		if err := r.UnreadByte(); err != nil {
			return nil, err
		}
		line, err := r.ReadBytes('\n')
		if err != nil {
			return nil, err
		}
		return bytes.Fields(trimCRLF(line)), nil
	}

	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	count, err := strconv.Atoi(string(trimCRLF(line)))
	if err != nil || count < 0 {
		return nil, errProtocol
	}

	args := make([][]byte, count)
	for i := 0; i < count; i++ {
		hdr, err := r.ReadBytes('\n')
		if err != nil {
			return nil, err
		}
		h := trimCRLF(hdr)
		if len(h) == 0 || h[0] != '$' {
			return nil, errProtocol
		}
		n, err := strconv.Atoi(string(h[1:]))
		if err != nil || n < 0 {
			return nil, errProtocol
		}
		// n payload bytes followed by CRLF.
		data := make([]byte, n+2)
		if _, err := io.ReadFull(r, data); err != nil {
			return nil, err
		}
		args[i] = data[:n]
	}
	return args, nil
}

// readReply reads exactly one RESP reply from r and appends its raw bytes
// (framing included) to buf, so the proxy can relay it verbatim. It recurses
// into arrays so nested replies are captured whole.
func readReply(r *bufio.Reader, buf *bytes.Buffer) error {
	line, err := readLineRaw(r, buf)
	if err != nil {
		return err
	}
	if len(line) == 0 {
		return errProtocol
	}

	switch line[0] {
	case '+', '-', ':':
		return nil
	case '$':
		n, err := strconv.Atoi(string(line[1:]))
		if err != nil {
			return errProtocol
		}
		if n < 0 {
			return nil // null bulk string ($-1)
		}
		// n payload bytes plus the trailing CRLF.
		if _, err := io.CopyN(buf, r, int64(n)+2); err != nil {
			return err
		}
		return nil
	case '*':
		n, err := strconv.Atoi(string(line[1:]))
		if err != nil {
			return errProtocol
		}
		if n < 0 {
			return nil // null array (*-1)
		}
		for i := 0; i < n; i++ {
			if err := readReply(r, buf); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("resp: unexpected reply type %q", line[0])
	}
}

// readLineRaw reads through the next '\n', appends the raw bytes (with CRLF) to
// buf, and returns the line with the trailing CRLF stripped.
func readLineRaw(r *bufio.Reader, buf *bytes.Buffer) ([]byte, error) {
	b, err := r.ReadBytes('\n')
	if len(b) > 0 {
		buf.Write(b)
	}
	if err != nil {
		return nil, err
	}
	return trimCRLF(b), nil
}

func trimCRLF(b []byte) []byte {
	if len(b) > 0 && b[len(b)-1] == '\n' {
		b = b[:len(b)-1]
	}
	if len(b) > 0 && b[len(b)-1] == '\r' {
		b = b[:len(b)-1]
	}
	return b
}

// encodeCommand serialises args back into a RESP multibulk frame for the backend.
func encodeCommand(args [][]byte) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n", len(a))
		b.Write(a)
		b.WriteString("\r\n")
	}
	return b.Bytes()
}

// encodeBulk renders a single bulk-string reply (used for `PING <msg>`).
func encodeBulk(v []byte) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "$%d\r\n", len(v))
	b.Write(v)
	b.WriteString("\r\n")
	return b.Bytes()
}
