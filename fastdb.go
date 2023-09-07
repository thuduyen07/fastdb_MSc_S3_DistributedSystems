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
	"time"
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

// ReplicaNode represents a node for data replication.
type ReplicaNode struct {
	ID       int    // ID của node (có thể dùng để xác định node)
	Address  string // Địa chỉ của node (ví dụ: IP:Port)
	Status   string // Trạng thái của node (ví dụ: "Active", "Inactive")
}

// ReplicationManager manages data replication to multiple nodes.
type ReplicationManager struct {
	Nodes []ReplicaNode // Danh sách các node sao lưu
	mu    sync.Mutex    // Mutex để đảm bảo đồng bộ hóa truy cập vào danh sách Nodes
	DB    *DB           // Thêm biến DB để sử dụng database
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

/* -------------------------- Methods/Functions Of Replication Manager ---------------------- */
// NewReplicationManager creates a new ReplicationManager with an empty list of nodes.
func NewReplicationManager(db *DB) *ReplicationManager {
	return &ReplicationManager{
		Nodes: []ReplicaNode{},
		DB:    db
	}
}

// AddNode adds a new node to the replication manager.
func (rm *ReplicationManager) AddNode(node ReplicaNode) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Check if the node already exists in the list
	for _, existingNode := range rm.Nodes {
		if existingNode.ID == node.ID {
			fmt.Printf("Node with ID %d already exists\n", node.ID)
			return
		}
	}

	rm.Nodes = append(rm.Nodes, node)
	fmt.Printf("Node added: %s\n", node.Address)
}

// RemoveNode removes a node from the replication manager by its ID.
func (rm *ReplicationManager) RemoveNode(nodeID int) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	for i, node := range rm.Nodes {
		if node.ID == nodeID {
			rm.Nodes = append(rm.Nodes[:i], rm.Nodes[i+1:]...)
			fmt.Printf("Node removed: %s\n", node.Address)
			return
		}
	}

	fmt.Printf("Node with ID %d not found\n", nodeID)
}

const maxRetries = 3

// Replicate sends data to replicate to all nodes with error handling and retry.
func (s *Server) Replicate(key int, bucket string, value []byte) {
	for _, node := range s.replicationManager.Nodes {
		success := false
		retries := 0

		for retries < maxRetries {
			success = s.replicateToNode(node, key, bucket, value)
			if success {
				break
			}

			// Xử lý lỗi sao chép và retry sau một khoảng thời gian
			fmt.Printf("Failed to replicate data to node %s. Retrying in 5 seconds...\n", node.Address)
			time.Sleep(5 * time.Second)
			retries++
		}

		if !success {
			fmt.Printf("Failed to replicate data to node %s after %d retries\n", node.Address, maxRetries)
		}
	}
}

// replicateToNode sends data to replicate to a specific node.
func (s *Server) replicateToNode(node ReplicaNode, key int, bucket string, value []byte) bool {
	// Thực hiện logic sao chép dữ liệu đến node sao lưu
	err := s.DB.Set(bucket, key, value)
	if err != nil {
		return false
	}

	return true
}

// ReceiveReplicationData stores key/value pair in the in-memory database.
func (s *Server) ReceiveReplicationData(key int, bucket string, value []byte) {
    // Sử dụng database DB đã khai báo ở đầu để lưu key/value pair
    err := s.DB.Set(bucket, key, value)
    if err != nil {
        fmt.Printf("Failed to store key/value pair in DB: %v\n", err)
    }
}

