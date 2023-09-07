package fastdb

/* ------------------------------- Imports --------------------------- */

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"bufio"
	"net"
	"strings"
	"time"
	"github.com/marcelloh/fastdb/persist"
)

// Server represents the key/value store server.
type Server struct {
	db    *DB
	mutex sync.Mutex
}

/* -------------------------- Methods/Functions Of Server ---------------------- */

// NewServer creates a new key/value store server.
func NewServer(db *DB) *Server {
	return &Server{
		db: db,
	}
}

// Start starts the server and listens for incoming client connections.
func (s *Server) Start(address string) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	defer listener.Close()

	fmt.Printf("Server listening on %s\n", address)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %s\n", err)
			continue
		}

		go s.handleConnection(conn)
	}
}

// handleConnection handles a single client connection.
func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	fmt.Printf("Client connected: %s\n", conn.RemoteAddr())

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		command := scanner.Text()
		fmt.Printf("Received command: %s\n", command)

		parts := strings.Fields(command)
		if len(parts) < 2 {
			conn.Write([]byte("Invalid command\n"))
			continue
		}

		switch parts[0] {
		case "GET":
			key, err := strconv.Atoi(parts[1])
			if err != nil {
				conn.Write([]byte("Invalid key\n"))
				continue
			}
			value, ok := s.db.Get(parts[2], key)
			if ok {
				conn.Write(value)
			} else {
				conn.Write([]byte("Key not found\n"))
			}
		case "SET":
			key, err := strconv.Atoi(parts[1])
			if err != nil {
				conn.Write([]byte("Invalid key\n"))
				continue
			}
			bucket := parts[2]
			value := parts[3]
			s.db.Set(bucket, key, []byte(value))
			conn.Write([]byte("OK\n"))
		case "DEL":
			key, err := strconv.Atoi(parts[1])
			if err != nil {
				conn.Write([]byte("Invalid key\n"))
				continue
			}
			bucket := parts[2]
			deleted, _ := s.db.Del(bucket, key)
			if deleted {
				conn.Write([]byte("OK\n"))
			} else {
				conn.Write([]byte("Key not found\n"))
			}
		default:
			conn.Write([]byte("Unknown command\n"))
		}
	}
}

// Close closes the server.
func (s *Server) Close() {
	panic("Close function is not implemented")
}