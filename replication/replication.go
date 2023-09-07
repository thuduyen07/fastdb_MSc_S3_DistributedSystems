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

// ReplicaNode represents a node for data replication.
type ReplicaNode struct {
	ID       int    // ID của node (có thể dùng để xác định node)
	Address  string // Địa chỉ của node (ví dụ: IP:Port)
	Status   string // Trạng thái của node (ví dụ: "Active", "Inactive")
}