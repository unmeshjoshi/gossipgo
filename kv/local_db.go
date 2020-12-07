// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package kv

import (
	"reflect"

	"gossipgo/storage"
)

// A LocalDB provides methods to access only a local, in-memory key
// value store. It utilizes a single storage/Range object, backed by
// a storage/InMem engine.
type LocalDB struct {
	rng *storage.Range
}

// NewLocalDB returns a local-only KV DB for direct access to a store.
func NewLocalDB(rng *storage.Range) *LocalDB {
	return &LocalDB{rng: rng}
}

// invokeMethod sends the specified RPC asynchronously and returns a
// channel which receives the reply struct when the call is
// complete. Returns a channel of the same type as "reply".
func (db *LocalDB) invokeMethod(method string, args, reply interface{}) interface{} {
	chanVal := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(reply)), 1)
	replyVal := reflect.ValueOf(reply)
	reflect.ValueOf(db.rng).MethodByName(method).Call([]reflect.Value{
		reflect.ValueOf(args),
		replyVal,
	})
	chanVal.Send(replyVal)

	return chanVal.Interface()
}

// Get passes through to local range.
func (db *LocalDB) Get(args *storage.GetRequest) <-chan *storage.GetResponse {
	return db.invokeMethod("Get",
		args, &storage.GetResponse{}).(chan *storage.GetResponse)
}

// Put passes through to local range.
func (db *LocalDB) Put(args *storage.PutRequest) <-chan *storage.PutResponse {
	return db.invokeMethod("Put",
		args, &storage.PutResponse{}).(chan *storage.PutResponse)
}



// Increment passes through to local range.
func (db *LocalDB) Increment(args *storage.IncrementRequest) <-chan *storage.IncrementResponse {
	return db.invokeMethod("Increment",
		args, &storage.IncrementResponse{}).(chan *storage.IncrementResponse)
}
