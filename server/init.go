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

package server

import (
	commander "github.com/nictuku/go-commander"
	"github.com/google/uuid"
	"gossipgo/storage"
	"log"
)

// A CmdInit command initializes a new Cockroach cluster.
var CmdInit = &commander.Command{
	UsageLine: "init <bootstrap-data-dir> <default-zone-config-filename>",
	Short:     "init new Cockroach cluster",
	Long: `
Initialize a new Cockroach cluster on this node. The cluster is
started with only a single replica, whose data is stored in the
directory specified by the first argument <bootstrap-data-dir>. The
format of the bootstrap data directory is given by the specification
below. Note that only SSD and HDD devices may be specified; in-memory
devices cannot be used to initialize a cluster.

  ssd=<data-dir> | hdd=<data-dir>

The provided zone configuration (specified by second argument
<default-zone-config-filename>) is installed as the default. In the
likely event that the default zone config provides for more than a
single replica, the first range will move to increase its replication
to the correct level upon start.

To start the cluster after initialization, run "cockroach start".
`,
	Run: runInit}

// runInit.
func runInit(cmd *commander.Command, args []string) {
	if len(args) != 2 {
		cmd.Usage()
		return
	}
	// Specifying the disk type as HDD may be incorrect, but doesn't
	// matter for this bootstrap step.
	engine, err := initEngine(args[0])
	if engine.Type() == storage.MEM {
		log.Print("Cannot initialize a cockroach cluster using an in-memory storage device")
	}
	if err != nil {
		log.Print(err)
	}
	// Generate a new cluster UUID.
	clusterID := uuid.New()
	if _, err := BootstrapCluster(clusterID.String(), engine); err != nil {
		log.Print(err)
	}
	// TODO(spencer): install the default zone config.
	log.Printf("Cockroach cluster %s has been initialized", clusterID)
	log.Printf(`To start the cluster, run "cockroach start"`)
}
