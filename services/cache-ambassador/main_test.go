package main

/*
Built-in self-test for the cache-ambassador RESP framing and routing.

It validates, with no external network:

  1. readReply captures every RESP2 reply type (+ - : $ *, including null bulk
     and nested arrays) byte-for-byte, even when the underlying stream delivers
     one byte per Read (proving partial-read handling).
  2. readCommand frames multibulk commands and returns them one-per-call from a
     single pipelined buffer, and also parses inline commands.
  3. End to end: a client speaks to server.handleConn over an in-memory pipe;
     dispatch answers PING/HELLO/CLIENT locally and routes GET/SET to the shard
     chosen by crc32(key)%N, relaying the backend reply verbatim. HELLO must be
     rejected so a go-redis v9 client falls back to RESP2.

Run with `go test ./...` (executed during the Docker build).
*/

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"testing"
	"time"
)

// oneByteReader yields its data a single byte per Read to exercise partial reads.
type oneByteReader struct {
	data []byte
	pos  int
}

func (r *oneByteReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	p[0] = r.data[r.pos]
	r.pos++
	return 1, nil
}

func TestReadReplyFramingPartial(t *testing.T) {
	cases := []string{
		"+OK\r\n",
		"-ERR nope\r\n",
		":42\r\n",
		"$3\r\nbar\r\n",
		"$-1\r\n",                             // null bulk
		"$0\r\n\r\n",                          // empty bulk
		"*2\r\n$3\r\nfoo\r\n:7\r\n",           // array with mixed elements
		"*2\r\n$3\r\nfoo\r\n*1\r\n:1\r\n",     // nested array
		"*-1\r\n",                             // null array
	}
	for _, want := range cases {
		br := bufio.NewReader(&oneByteReader{data: []byte(want)})
		var buf bytes.Buffer
		if err := readReply(br, &buf); err != nil {
			t.Fatalf("readReply(%q) error: %v", want, err)
		}
		if got := buf.String(); got != want {
			t.Fatalf("readReply relay mismatch: got %q want %q", got, want)
		}
		if _, err := br.ReadByte(); err != io.EOF {
			t.Fatalf("readReply(%q) did not consume exactly one reply (err=%v)", want, err)
		}
	}
}

func TestReadCommandPipelinedAndInline(t *testing.T) {
	// Two multibulk commands back-to-back, delivered one byte at a time.
	stream := "*1\r\n$4\r\nPING\r\n" +
		"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"
	br := bufio.NewReader(&oneByteReader{data: []byte(stream)})

	first, err := readCommand(br)
	if err != nil {
		t.Fatalf("first readCommand: %v", err)
	}
	if len(first) != 1 || string(first[0]) != "PING" {
		t.Fatalf("first command = %q, want [PING]", first)
	}
	second, err := readCommand(br)
	if err != nil {
		t.Fatalf("second readCommand: %v", err)
	}
	if len(second) != 3 || string(second[0]) != "SET" || string(second[1]) != "k" || string(second[2]) != "v" {
		t.Fatalf("second command = %q, want [SET k v]", second)
	}

	// Inline form.
	inline := bufio.NewReader(bytes.NewReader([]byte("PING\r\n")))
	cmd, err := readCommand(inline)
	if err != nil {
		t.Fatalf("inline readCommand: %v", err)
	}
	if len(cmd) != 1 || string(cmd[0]) != "PING" {
		t.Fatalf("inline command = %q, want [PING]", cmd)
	}
}

// fakeShard answers commands so the proxy can be exercised without real Redis.
// GET returns the shard's own address as the value, which lets the test assert
// which shard a key was routed to. Every other command gets +OK.
func fakeShard(nc net.Conn, addr string) {
	defer nc.Close()
	br := bufio.NewReader(nc)
	for {
		args, err := readCommand(br)
		if err != nil {
			return
		}
		var reply []byte
		if len(args) > 0 && string(bytesUpper(args[0])) == "GET" {
			reply = encodeBulk([]byte(addr))
		} else {
			reply = []byte("+OK\r\n")
		}
		if _, err := nc.Write(reply); err != nil {
			return
		}
	}
}

func bytesUpper(b []byte) []byte {
	out := make([]byte, len(b))
	for i, c := range b {
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		out[i] = c
	}
	return out
}

func TestEndToEndRoutingAndHandshake(t *testing.T) {
	dial := func(addr string) (net.Conn, error) {
		c1, c2 := net.Pipe()
		go fakeShard(c2, addr)
		return c1, nil
	}
	shards := []string{"shard-a:6379", "shard-b:6379", "shard-c:6379"}
	srv := newServer(shards, dial)

	cliServer, cliClient := net.Pipe()
	srv.wg.Add(1)
	go srv.handleConn(cliServer)
	defer cliClient.Close()

	clientBR := bufio.NewReader(cliClient)
	send := func(cmd []byte) string {
		_ = cliClient.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := cliClient.Write(cmd); err != nil {
			t.Fatalf("write: %v", err)
		}
		var buf bytes.Buffer
		if err := readReply(clientBR, &buf); err != nil {
			t.Fatalf("read reply: %v", err)
		}
		return buf.String()
	}

	// Handshake tolerance.
	if got := send(encodeCommand([][]byte{[]byte("HELLO"), []byte("3")})); got != "-ERR unknown command 'HELLO'\r\n" {
		t.Fatalf("HELLO reply = %q", got)
	}
	if got := send(encodeCommand([][]byte{[]byte("CLIENT"), []byte("SETINFO"), []byte("lib-name"), []byte("go-redis")})); got != "+OK\r\n" {
		t.Fatalf("CLIENT reply = %q", got)
	}
	if got := send(encodeCommand([][]byte{[]byte("PING")})); got != "+PONG\r\n" {
		t.Fatalf("PING reply = %q", got)
	}

	// SET is relayed and answered +OK.
	if got := send(encodeCommand([][]byte{[]byte("SET"), []byte("cart:cust_1"), []byte("v")})); got != "+OK\r\n" {
		t.Fatalf("SET reply = %q", got)
	}

	// GET must land on the crc32-selected shard, and the reply is relayed verbatim.
	for _, key := range []string{"cart:cust_1", "cart:cust_2", "session:x", "a", "b", "c"} {
		wantShard := srv.shardFor([]byte(key)).addr
		got := send(encodeCommand([][]byte{[]byte("GET"), []byte(key)}))
		if want := string(encodeBulk([]byte(wantShard))); got != want {
			t.Fatalf("GET %q routed wrong: got %q want %q", key, got, want)
		}
	}
}
