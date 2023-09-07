package fastdb

/* ------------------------------- Imports --------------------------- */

import (
	"errors"
	"fmt"
	"strconv"
	"bufio"
	"net"
	"strings"
	"sync"
	"time"
	"github.com/marcelloh/fastdb/persist"
	"hash/crc32"
)

// ReplicaNode represents a node for data replication.
type ReplicaNode struct {
	Id       int    // ID của node (có thể dùng để xác định node)
	Address  string // Địa chỉ của node (ví dụ: IP:Port)
	Status   string // Trạng thái của node (ví dụ: "Active", "Inactive")
	HashId uint32
}

// NewReplicaNode tạo một đối tượng ReplicaNode mới với các thông tin cơ bản.
func NewReplicaNode(Id int, Address string, Status string) *ReplicaNode {
	return &ReplicaNode{
		Id:      Id,
		Address: Address,
		Status:  Status,
		HashId:  crc32.Checksum([]byte(Id)),
	}
}

