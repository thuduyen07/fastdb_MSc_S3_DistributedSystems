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

// ReplicationManager manages data replication to multiple nodes.
type ReplicationManager struct {
	Nodes []ReplicaNode // Danh sách các node sao lưu
	mu    sync.Mutex    // Mutex để đảm bảo đồng bộ hóa truy cập vào danh sách Nodes
	DB    *DB           // Thêm biến DB để sử dụng database
}

/* -------------------------- Methods/Functions Of Replication Manager ---------------------- */
// NewReplicationManager creates a new ReplicationManager with an empty list of nodes.
func NewReplicationManager(db *DB) *ReplicationManager {
	return &ReplicationManager{
		Nodes: []ReplicaNode{},
		DB:    db,
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

