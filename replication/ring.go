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

/* ---------------------- Constants/Types/Variables ------------------ */

// Ring is a network of distributed nodes.
type Ring struct {
	Nodes Nodes
}

// Nodes is an array of nodes.
type Nodes []ReplicaNode

// Initializes new distribute network of nodes or a ring.
func NewRing() *Ring {
	return &Ring{Nodes : Nodes{}}
}

func (r *Ring) AddNode(id string, address string, status string) {
	node := NewReplicaNode(id, address, status )  
	r.Nodes = append(r.Nodes, node)
	sort.Sort(r.Nodes)
}

func (n Nodes) Len() int           { return len(n) }
func (n Nodes) Less(i, j int) bool { return n[i].HashId < n[j].HashId }
func (n Nodes) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }

// Removes node from the ring if it exists, else returns 
// ErrNodeNotFound.
func (r *Ring) RemoveNode(id string) error {
    for i, node := range r.Nodes {
        if node.Id == ID {
            r.Nodes = append(r.Nodes[:i], r.Nodes[i+1:]...)
            return nil
        }
    }
    return errors.New("Node not found")
}

// Gets node which is mapped to the key. Return value is identifer
// of the node given in `AddNode`.
func (r *Ring) Get(key string) string {
	searchfn := func(i int) bool {
	  return r.Nodes[i].HashId >= crc32.Checksum([]byte(key))
	}
	i := sort.Search(r.Nodes.Len(), searchfn)
	if i >= r.Nodes.Len() {
	  i = 0
	}
	return r.Nodes[i].Id
}