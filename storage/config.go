package storage

import (
	"gossipgo/gossip"
	"net"
	"gossipgo/util"
)

// DiskType is the type of a disk that a Store is storing data on.
type DiskType uint32

// Replica describes a replica location by node ID (corresponds to a
// host:port via lookup on gossip network), store ID (corresponds to
// a physical device, unique per node) and range ID. Datacenter and
// DiskType are provided to optimize reads. Replicas are stored in
// Range lookup records (meta1, meta2).
type Replica struct {
	NodeID     int32
	StoreID    int32
	RangeID    int64
	Datacenter string
	DiskType
}

const (
	// SSD = Solid State Disk
	SSD DiskType = iota
	// HDD = Spinning disk
	HDD
	// MEM = DRAM
	MEM
)

// StoreCapacity contains capacity information for a storage device.
type StoreCapacity struct {
	Capacity  int64
	Available int64
	DiskType  DiskType
}

// NodeAttributes holds details on node physical/network topology.
type NodeAttributes struct {
	NodeID     int32
	Address    net.Addr
	Datacenter string
	PDU        string
	Rack       string
}

// StoreAttributes holds store information including physical/network
// topology via NodeAttributes and disk type & capacity data.
type StoreAttributes struct {
	StoreID    int32
	Attributes NodeAttributes
	Capacity   StoreCapacity
}

// ZoneConfig holds configuration that is needed for a range of KV pairs.
type ZoneConfig struct {
	// Replicas is a map from datacenter name to a slice of disk types.
	Replicas      map[string]([]DiskType) `yaml:"replicas,omitempty"`
	RangeMinBytes int64                   `yaml:"range_min_bytes,omitempty"`
	RangeMaxBytes int64                   `yaml:"range_max_bytes,omitempty"`
}

// Less compares two StoreAttributess based on percentage of disk available.
func (a StoreAttributes) Less(b gossip.Ordered) bool {
	return a.Capacity.PercentAvail() < b.(StoreAttributes).Capacity.PercentAvail()
}

// PercentAvail computes the percentage of disk space that is available.
func (sc StoreCapacity) PercentAvail() float64 {
	return float64(sc.Available) / float64(sc.Capacity)
}

// RangeLocations is the metadata value stored for a metadata key.
// The metadata key has meta1 or meta2 key prefix and the suffix encodes
// the end key of the range this struct represents.
type RangeLocations struct {
	// The start key of the range represented by this struct, along with the
	// meta1 or meta2 key prefix.
	StartKey Key
	Replicas []Replica
}

// ChooseRandomReplica returns a replica selected at random or nil if none exist.
func ChooseRandomReplica(replicas []Replica) *Replica {
	if len(replicas) == 0 {
		return nil
	}
	r := util.CachedRand
	return &replicas[r.Intn(len(replicas))]
}
