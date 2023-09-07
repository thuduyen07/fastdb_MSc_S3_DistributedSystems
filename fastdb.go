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
	"sync"
	"github.com/marcelloh/fastdb/persist"
)

/* ---------------------- Constants/Types/Variables ------------------ */

// DB represents a collection of key-value pairs that persist on disk or memory.
type DB struct {
	aof  *persist.AOF
	keys map[string]map[int][]byte
	mu   sync.RWMutex
}

// Server represents the key/value store server.
type Server struct {
	db    *DB
	mutex sync.Mutex
}
/* -------------------------- Methods/Functions Of Database ---------------------- */

/*
Open opens a database at the provided path.
If the file doesn't exist, it will be created automatically.
If the path is ':memory:' then the database will be opened in memory only.
*/
func Open(path string, syncIime int) (*DB, error) {
	var (
		aof *persist.AOF
		err error
	)

	keys := make(map[string]map[int][]byte)

	if path != ":memory:" {
		aof, keys, err = persist.OpenPersister(path, syncIime)
	}

	return &DB{aof: aof, keys: keys}, err //nolint:wrapcheck // it is already wrapped
}

/*
Defrag optimises the file to reflect the latest state.
*/
func (fdb *DB) Defrag() error {
	fdb.mu.Lock()
	defer fdb.mu.Unlock()

	var err error

	err = fdb.aof.Defrag(fdb.keys)
	if err != nil {
		err = fmt.Errorf("defrag error: %w", err)
	}

	return err
}

/*
Del deletes one map value in a bucket.
*/
func (fdb *DB) Del(bucket string, key int) (bool, error) {
	var err error

	fdb.mu.Lock()
	defer fdb.mu.Unlock()

	// bucket exists?
	_, found := fdb.keys[bucket]
	if !found {
		return found, nil
	}

	// key exists in bucket?
	_, found = fdb.keys[bucket][key]
	if !found {
		return found, nil
	}

	if fdb.aof != nil {
		lines := "del\n" + bucket + "_" + strconv.Itoa(key) + "\n"

		err = fdb.aof.Write(lines)
		if err != nil {
			return false, fmt.Errorf("del->write error: %w", err)
		}
	}

	delete(fdb.keys[bucket], key)

	if len(fdb.keys[bucket]) == 0 {
		delete(fdb.keys, bucket)
	}

	return true, nil
}

/*
Get returns one map value from a bucket.
*/
func (fdb *DB) Get(bucket string, key int) ([]byte, bool) {
	fdb.mu.RLock()
	defer fdb.mu.RUnlock()

	data, ok := fdb.keys[bucket][key]

	return data, ok
}

/*
GetAll returns all map values from a bucket.
*/
func (fdb *DB) GetAll(bucket string) (map[int][]byte, error) {
	fdb.mu.RLock()
	defer fdb.mu.RUnlock()

	bmap, found := fdb.keys[bucket]
	if !found {
		return nil, errors.New("bucket not found")
	}

	return bmap, nil
}

/*
Info returns info about the storage.
*/
func (fdb *DB) Info() string {
	count := 0
	for i := range fdb.keys {
		count += len(fdb.keys[i])
	}

	return fmt.Sprintf("%d record(s) in %d bucket(s)", count, len(fdb.keys))
}

/*
Set stores one map value in a bucket.
*/
func (fdb *DB) Set(bucket string, key int, value []byte) error {
	fdb.mu.Lock()
	defer fdb.mu.Unlock()

	if fdb.aof != nil {
		lines := "set\n" + bucket + "_" + strconv.Itoa(key) + "\n" + string(value) + "\n"

		err := fdb.aof.Write(lines)
		if err != nil {
			return fmt.Errorf("sel->write error: %w", err)
		}
	}

	_, found := fdb.keys[bucket]
	if !found {
		fdb.keys[bucket] = map[int][]byte{}
	}

	fdb.keys[bucket][key] = value

	return nil
}

/*
Close closes the database.
*/
func (fdb *DB) Close() error {
	if fdb.aof != nil {
		fdb.mu.Lock()
		defer fdb.mu.Unlock()

		err := fdb.aof.Close()
		if err != nil {
			return fmt.Errorf("close error: %w", err)
		}
	}

	fdb.keys = make(map[string]map[int][]byte)

	return nil
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
