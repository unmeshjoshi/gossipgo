package kv

import (
	"bytes"
	"encoding/gob"
	"gossipgo/gossip"
	"gossipgo/rpc"
	"gossipgo/storage"
	"gossipgo/util"
	net "net"
	"reflect"
	"time"
)

// A DB interface provides asynchronous methods to access a key value store.
type DB interface {
	Get(args *storage.GetRequest) <-chan *storage.GetResponse
	Put(args *storage.PutRequest) <-chan *storage.PutResponse
	Increment(args *storage.IncrementRequest) <-chan *storage.IncrementResponse
}

// A DistDB provides methods to access Cockroach's monolithic,
// distributed key value store. Each method invocation triggers a
// lookup or lookups to find replica metadata for implicated key
// ranges.
type DistDB struct {
	// gossip provides up-to-date information about the start of the
	// key range, used to find the replica metadata for arbitrary key
	// ranges.
	gossip *gossip.Gossip
	// rangeCache caches replica metadata for key ranges. The cache is
	// filled while servicing read and write requests to the key value
	// store.
	rangeCache util.LRUCache
}

// PutI sets the given key to the serialized byte string of the value
// provided. Uses current time and default expiration.
func PutI(db DB, key storage.Key, value interface{}) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(value); err != nil {
		return err
	}
	pr := <-db.
		Put(&storage.PutRequest{
		Key: key,
		Value: storage.Value{
			Bytes:     buf.Bytes(),
			Timestamp: time.Now().UnixNano(),
		},
	})
	return pr.Error
}

// NewDB returns a key-value datastore client which connects to the
// Cockroach cluster via the supplied gossip instance.
func NewDB(gossip *gossip.Gossip) *DistDB {
	return &DistDB{gossip: gossip}
}

func (db *DistDB) nodeIDToAddr(nodeID int32) (net.Addr, error) {
	nodeIDKey := gossip.MakeNodeIDGossipKey(nodeID)
	info, err := db.gossip.GetInfo(nodeIDKey)
	if info == nil || err != nil {
		return nil, util.Errorf("Unable to lookup address for node: %v. Error: %v", nodeID, err)
	}
	return info.(net.Addr), nil
}

func (db *DistDB) lookupMetadata(metadataKey storage.Key, replicas []storage.Replica) (*storage.RangeLocations, error) {
	replica := storage.ChooseRandomReplica(replicas)
	if replica == nil {
		return nil, util.Errorf("No replica to choose for metadata key: %q", metadataKey)
	}

	addr, err := db.nodeIDToAddr(replica.NodeID)
	if err != nil {
		// TODO(harshit): May be retry a different replica.
		return nil, err
	}
	client := rpc.NewClient(addr)
	arg := &storage.InternalRangeLookupRequest{
		RequestHeader: storage.RequestHeader{
			Replica: *replica,
		},
		Key: metadataKey,
	}
	var reply storage.InternalRangeLookupResponse
	err = client.Call("Node.InternalRangeLookup", arg, &reply)
	if err != nil {
		return nil, err
	}
	if reply.Error != nil {
		return nil, reply.Error
	}
	return &reply.Locations, nil
}

// TODO(harshit): Consider caching returned metadata info.
func (db *DistDB) lookupMeta1(key storage.Key) (*storage.RangeLocations, error) {
	info, err := db.gossip.GetInfo(gossip.KeyFirstRangeMetadata)
	if err != nil {
		return nil, err
	}
	metadataKey := storage.MakeKey(storage.KeyMeta1Prefix, key)
	return db.lookupMetadata(metadataKey, info.(storage.RangeLocations).Replicas)
}

func (db *DistDB) lookupMeta2(key storage.Key) (*storage.RangeLocations, error) {
	meta1Val, err := db.lookupMeta1(key)
	if err != nil {
		return nil, err
	}
	metadataKey := storage.MakeKey(storage.KeyMeta2Prefix, key)
	return db.lookupMetadata(metadataKey, meta1Val.Replicas)
}

// getNode gets an RPC client to the node where the requested
// key is located. The range cache may be updated. The bi-level range
// metadata for the cluster is consulted in the event that the local
// cache doesn't contain range metadata corresponding to the specified
// key.
func (db *DistDB) getNode(key storage.Key) (*rpc.Client, *storage.Replica, error) {
	meta2Val, err := db.lookupMeta2(key)
	if err != nil {
		return nil, nil, err
	}
	replica := storage.ChooseRandomReplica(meta2Val.Replicas)
	if replica == nil {
		return nil, nil, util.Errorf("No node found for key: %q", key)
	}
	addr, err := db.nodeIDToAddr(replica.NodeID)
	if err != nil {
		// TODO(harshit): May be retry a different replica.
		return nil, nil, err
	}
	return rpc.NewClient(addr), replica, nil
}

// sendRPC sends the specified RPC asynchronously and returns a
// channel which receives the reply struct when the call is
// complete. Returns a channel of the same type as "reply".
func (db *DistDB) sendRPC(key storage.Key, method string, args, reply interface{}) interface{} {
	chanVal := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(reply)), 1)

	go func() {
		replyVal := reflect.ValueOf(reply)
		node, replica, err := db.getNode(key)
		if err == nil {
			argsHeader := args.(*storage.RequestHeader)
			argsHeader.Replica = *replica
			err = node.Call(method, args, reply)
		}
		if err != nil {
			// TODO(spencer): check error here; we need to clear this
			// segment of range cache and retry getNode() if the range
			// wasn't found.
			reflect.Indirect(replyVal).FieldByName("Error").Set(reflect.ValueOf(err))
		}
		chanVal.Send(replyVal)
	}()

	return chanVal.Interface()
}

// Get .
func (db *DistDB) Get(args *storage.GetRequest) <-chan *storage.GetResponse {
	return db.sendRPC(args.Key, "Node.Get",
		args, &storage.GetResponse{}).(chan *storage.GetResponse)
}

// Put .
func (db *DistDB) Put(args *storage.PutRequest) <-chan *storage.PutResponse {
	return db.sendRPC(args.Key, "Node.Put",
		args, &storage.PutResponse{}).(chan *storage.PutResponse)
}


// Increment .
func (db *DistDB) Increment(args *storage.IncrementRequest) <-chan *storage.IncrementResponse {
	return db.sendRPC(args.Key, "Node.Increment",
		args, &storage.IncrementResponse{}).(chan *storage.IncrementResponse)
}


// BootstrapRangeLocations sets meta1 and meta2 values for KeyMax,
// using the provided replica.
func BootstrapRangeLocations(db DB, replica storage.Replica) error {
	locations := storage.RangeLocations{
		Replicas: []storage.Replica{replica},
		// TODO(spencer): uncomment when we have hrsht's change.
		//StartKey: storage.KeyMin,
	}
	// Write meta1.
	if err := PutI(db, storage.MakeKey(storage.KeyMeta1Prefix, storage.KeyMax), locations); err != nil {
		return err
	}
	// Write meta2.
	if err := PutI(db, storage.MakeKey(storage.KeyMeta2Prefix, storage.KeyMax), locations); err != nil {
		return err
	}
	return nil
}
