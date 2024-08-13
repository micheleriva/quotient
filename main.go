package main

import (
	"fmt"
	"log"
	"sync"
)

var (
	Configuration  *Config
	QF             *QuotientFilter
	ServerRaftNode *RaftNode
)

func init() {
	config, err := ParseConfigFile()
	if err != nil {
		fmt.Println(err)
	}

	Configuration = config

	log.Printf("Loaded configuration:")
	log.Printf("Server Host: %s", config.Server.Host)
	log.Printf("Server Port: %d", config.Server.Port)
	log.Printf("Raft Node ID: %s", config.Raft.NodeID)
	log.Printf("Raft TCP Address: %s", config.Raft.TCPAddress)
	log.Printf("Raft Log Directory: %s", config.Raft.LogDir)
	log.Printf("Raft Snapshot Directory: %s", config.Raft.SnapshotDir)
	log.Printf("Raft Timeout: %s", config.Raft.Timeout)
	log.Printf("QF Size: %d", uint64(1)<<config.Quotient.LogSize)
	log.Println()

	QF = NewQuotientFilter(config.Quotient.LogSize)
}

func main() {
	log.Printf("Creating Raft node with log directory: %s", Configuration.Raft.LogDir)
	log.Printf("Creating Raft node with snapshot directory: %s", Configuration.Raft.SnapshotDir)

	raftNode, err := NewRaftNode(Configuration, QF)
	if err != nil {
		log.Fatalf("Failed to create Raft node: %v", err)
	}
	ServerRaftNode = raftNode

	log.Printf("Boostrapping cluster with peers %s", Configuration.Raft.PeerAddresses)
	if err := raftNode.BootstrapCluster(Configuration.Raft.PeerAddresses); err != nil {
		log.Printf("Failed to bootstrap cluster: %v", err)
	}

	if err := raftNode.Start(); err != nil {
		log.Fatalf("Failed to start Raft node: %v", err)
	}
	log.Println("Raft node started successfully")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		StartServer(Configuration)
	}()

	wg.Wait()
}
