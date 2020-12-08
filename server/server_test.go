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
// Author: Andrew Bonventre (andybons@gmail.com)

package server

import (
	"compress/gzip"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"gossipgo/storage"
	"log"
)

var (
	s    *server
	once sync.Once
)

func startServer() *server {
	once.Do(func() {
		resetTestData()
		s, err := newServer()
		if err != nil {
			log.Print(err)
		}
		engines := []storage.Engine{storage.NewInMem(1 << 20)}
		if _, err := BootstrapCluster("cluster-1", engines[0]); err != nil {
			log.Print(err)
		}
		s.gossip.SetBootstrap([]net.Addr{s.rpc.Addr})
		go func() {
			log.Print(s.start(engines)) // TODO(spencer): should shutdown server.
		}()
		log.Printf("Test server listening on http: %s, rpc: %s", *httpAddr, *rpcAddr)
	})

	return s
}

func resetTestData() {
	// TODO(spencer): remove all data files once rocksdb is hooked up.
}

// TestHealthz verifies that /_admin/healthz does, in fact, return "ok"
// as expected.
func TestHealthz(t *testing.T) {
	startServer()
	defer resetTestData()
	time.Sleep(2 * time.Second)
	url := "http://" + *httpAddr + "/_admin/healthz"
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("error requesting healthz at %s: %s", url, err)
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("could not read response body: %s", err)
	}
	expected := "ok"
	if !strings.Contains(string(b), expected) {
		t.Errorf("expected body to contain %q, got %q", expected, string(b))
	}
}

// TestGzip hits the /_admin/healthz endpoint while explicitly disabling
// decompression on a custom client's Transport and setting it
// conditionally via the request's Accept-Encoding headers.
func TestGzip(t *testing.T) {
	startServer()
	defer resetTestData()
	time.Sleep(2 * time.Second) //TODO: wait for http server to start listening
	client := http.Client{
		Transport: &http.Transport{
			Proxy:              http.ProxyFromEnvironment,
			DisableCompression: true,
		},
	}
	req, err := http.NewRequest("GET", "http://"+*httpAddr+"/_admin/healthz", nil)
	if err != nil {
		t.Fatalf("could not create request: %s", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("could not make request to %s: %s", req.URL, err)
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("could not read response body: %s", err)
	}
	expected := "ok"
	if !strings.Contains(string(b), expected) {
		t.Errorf("expected body to contain %q, got %q", expected, string(b))
	}
	// Test for gzip explicitly.
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("could not make request to %s: %s", req.URL, err)
	}
	defer resp.Body.Close()
	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		t.Fatalf("could not create new gzip reader: %s", err)
	}
	b, err = ioutil.ReadAll(gz)
	if err != nil {
		t.Fatalf("could not read gzipped response body: %s", err)
	}
	if !strings.Contains(string(b), expected) {
		t.Errorf("expected body to contain %q, got %q", expected, string(b))
	}
}
