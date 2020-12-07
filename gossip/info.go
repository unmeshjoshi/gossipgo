package gossip

import (
	"log"
	"net"
	"strings"
)

// Ordered is used to compare info values when managing info groups.
// Info values which are not int64, float64 or string must implement
// this interface to be used with groups.
type Ordered interface {
	// Returns true if the supplied Ordered value is less than this
	// object.
	Less(b Ordered) bool
}

// info is the basic unit of information traded over the gossip
// network.
type info struct {
	Key       string      // Info key
	Val       interface{} // Info value: must be one of {int64, float64, string}
	Timestamp int64       // Wall time at origination (Unix-nanos)
	TTLStamp  int64       // Wall time before info is discarded (Unix-nanos)
	Hops      uint32      // Number of hops from originator
	NodeAddr  net.Addr    // Originating node in "host:port" format
	peerAddr  net.Addr    // Proximate peer which passed us the info
	seq       int64       // Sequence number for incremental updates
}

// infoPrefix returns the text preceding the last period within
// the given key.
func infoPrefix(key string) string {
	if index := strings.LastIndex(key, "."); index != -1 {
		return key[:index]
	}
	return ""
}

// less returns true if i's value is less than b's value. i's and
// b's types must match.
func (i *info) less(b *info) bool {
	switch t := i.Val.(type) {
	case int64:
		return t < b.Val.(int64)
	case float64:
		return t < b.Val.(float64)
	case string:
		return t < b.Val.(string)
	default:
		if ord, ok := i.Val.(Ordered); ok {
			return ord.Less(b.Val.(Ordered))
		}
		log.Fatalf("unhandled info value type: %s", t)
	}
	return false
}

// expired returns true if the node's time to live (TTL) has expired.
func (i *info) expired(now int64) bool {
	return i.TTLStamp <= now
}

// isFresh returns true if the info has a sequence number newer
// than seq and wasn't either passed directly or originated from
// the same address as addr.
func (i *info) isFresh(addr net.Addr, seq int64) bool {
	if i.seq <= seq {
		return false
	}
	if i.NodeAddr.String() == addr.String() {
		return false
	}
	if i.peerAddr.String() == addr.String() {
		return false
	}
	return true
}

// infoMap is a map of keys to info object pointers.
type infoMap map[string]*info

// infoArray is a slice of Info object pointers.
type infoArray []*info

// Implement sort.Interface for infoArray.
func (a infoArray) Len() int           { return len(a) }
func (a infoArray) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a infoArray) Less(i, j int) bool { return a[i].less(a[j]) }
