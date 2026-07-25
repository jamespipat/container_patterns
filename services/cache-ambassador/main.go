// Command cache-ambassador is a RESP-subset proxy that sits on localhost next
// to the order-api container (the Ambassador sidecar pattern). The app talks to
// it as if it were a single Redis; the ambassador hash-shards each key across N
// backend Redis shards and relays replies verbatim, so the app stays oblivious
// to the number of shards or their DNS names.
//
// Supported commands: PING, GET, SET (with optional EX), SETEX, DEL. Routing:
// shard = crc32(key) % len(SHARDS). Handshake tolerance for go-redis v9: HELLO
// is rejected with "-ERR unknown command 'HELLO'" (so the client falls back to
// RESP2) and any CLIENT ... subcommand is answered "+OK".
package main

import (
	"bufio"
	"context"
	"fmt"
	"hash/crc32"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

const defaultShards = "cache-shard-0.cache-shards:6379,cache-shard-1.cache-shards:6379,cache-shard-2.cache-shards:6379"

// drainTimeout bounds how long we wait for in-flight client connections to
// finish after SIGTERM before exiting.
const drainTimeout = 10 * time.Second

type config struct {
	listenAddr string
	shards     []string
}

func loadConfig() (config, error) {
	cfg := config{
		listenAddr: getenv("LISTEN_ADDR", "127.0.0.1:6380"),
	}
	for _, s := range strings.Split(getenv("SHARDS", defaultShards), ",") {
		if s = strings.TrimSpace(s); s != "" {
			cfg.shards = append(cfg.shards, s)
		}
	}
	if len(cfg.shards) == 0 {
		return cfg, fmt.Errorf("SHARDS is empty")
	}
	return cfg, nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// server owns the shard pools and routes commands.
type server struct {
	shards []*pool
	wg     sync.WaitGroup // tracks active client connections
}

func newServer(shardAddrs []string, dial dialFunc) *server {
	s := &server{shards: make([]*pool, len(shardAddrs))}
	for i, addr := range shardAddrs {
		s.shards[i] = newPool(addr, dial)
	}
	return s
}

// shardFor picks the backend shard for a key. crc32 IEEE (the stdlib default,
// crc32.ChecksumIEEE) is used purely as an internal, deterministic hash; the
// mapping only has to be stable within this process.
func (s *server) shardFor(key []byte) *pool {
	return s.shards[crc32.ChecksumIEEE(key)%uint32(len(s.shards))]
}

// dispatch handles one command and returns the raw reply bytes plus whether the
// connection should be closed afterward (QUIT).
func (s *server) dispatch(args [][]byte) (reply []byte, closeConn bool) {
	name := strings.ToUpper(string(args[0]))
	switch name {
	case "PING":
		if len(args) >= 2 {
			return encodeBulk(args[1]), false
		}
		return []byte("+PONG\r\n"), false
	case "QUIT":
		return []byte("+OK\r\n"), true
	case "HELLO":
		// Reject so go-redis v9 falls back to RESP2.
		return []byte("-ERR unknown command 'HELLO'\r\n"), false
	case "CLIENT":
		// Accept CLIENT SETINFO / SETNAME / etc. as no-ops.
		return []byte("+OK\r\n"), false
	case "GET", "SET", "SETEX", "DEL":
		if len(args) < 2 {
			return []byte(fmt.Sprintf("-ERR wrong number of arguments for '%s' command\r\n", strings.ToLower(name))), false
		}
		return s.forward(args[1], args), false
	default:
		return []byte(fmt.Sprintf("-ERR unknown command '%s'\r\n", string(args[0]))), false
	}
}

// forward routes a keyed command to its shard and returns the shard's reply. On
// a transport failure it synthesises a RESP error so the client sees a clean
// reply instead of a dropped connection. Command payloads are never logged
// (they may hold user data).
func (s *server) forward(key []byte, args [][]byte) []byte {
	p := s.shardFor(key)
	reply, err := p.do(encodeCommand(args))
	if err != nil {
		log.Printf("shard %s error: %v", p.addr, err)
		return []byte("-ERR ambassador: upstream shard unavailable\r\n")
	}
	return reply
}

func (s *server) handleConn(nc net.Conn) {
	defer s.wg.Done()
	defer nc.Close()

	br := bufio.NewReader(nc)
	bw := bufio.NewWriter(nc)
	for {
		args, err := readCommand(br)
		if err != nil {
			return // EOF or protocol error: drop the connection
		}
		if len(args) == 0 {
			continue
		}

		reply, closeConn := s.dispatch(args)
		if _, err := bw.Write(reply); err != nil {
			return
		}
		// Only flush once the client's pipelined batch is drained, so a burst
		// of commands is answered with a single write.
		if closeConn || br.Buffered() == 0 {
			if err := bw.Flush(); err != nil {
				return
			}
		}
		if closeConn {
			return
		}
	}
}

// serve accepts connections until ctx is cancelled, then waits (up to
// drainTimeout) for in-flight connections to finish.
func (s *server) serve(ctx context.Context, ln net.Listener) {
	go func() {
		<-ctx.Done()
		_ = ln.Close() // unblock Accept
	}()

	for {
		nc, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				break // shutting down
			}
			log.Printf("accept error: %v", err)
			continue
		}
		s.wg.Add(1)
		go s.handleConn(nc)
	}

	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(drainTimeout):
		log.Printf("drain timeout: exiting with active connections")
	}
	for _, p := range s.shards {
		p.closeAll()
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)

	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	ln, err := net.Listen("tcp", cfg.listenAddr)
	if err != nil {
		log.Fatalf("listen %s: %v", cfg.listenAddr, err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	srv := newServer(cfg.shards, tcpDial)
	log.Printf("cache-ambassador listening on %s, %d shard(s): %s",
		cfg.listenAddr, len(cfg.shards), strings.Join(cfg.shards, ", "))
	srv.serve(ctx, ln)
	log.Printf("cache-ambassador stopped")
}
