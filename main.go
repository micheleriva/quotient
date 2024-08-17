package main

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"time"
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

	if Configuration.Raft.NodeID == "node1" {
		time.Sleep(5 * time.Second) // @todo: remove sleep
		log.Println("Node1 attempting to bootstrap cluster")
		if err := raftNode.BootstrapCluster(Configuration.Raft.PeerAddresses); err != nil {
			log.Printf("Failed to bootstrap cluster: %v", err)
		}
	} else {
		log.Println("Non-bootstrap node waiting to join cluster")
		time.Sleep(10 * time.Second) // @todo: remove sleep
	}

	// @todo: remove sleep
	time.Sleep(10 * time.Second)

	_, portStr, err := net.SplitHostPort(Configuration.Raft.TCPAddress)
	if err != nil {
		log.Fatalf("Failed to parse Raft TCP address: %v", err)
	}
	raftPort, err := strconv.Atoi(portStr)
	if err != nil {
		log.Fatalf("Failed to parse Raft port: %v", err)
	}

	raftAddr := fmt.Sprintf("%s:%d", "0.0.0.0", raftPort)
	go func() {
		log.Printf("Starting Raft server on: %s", raftAddr)
		if err := ServerRaftNode.Start(); err != nil {
			log.Fatalf("Failed to start Raft server: %v", err)
		}
	}()

	go func() {
		for {
			if raftNode.IsLeader() {
				log.Println("This node is the current leader")
			} else {
				log.Printf("Current leader: %s", raftNode.LeaderAddress())
			}
			time.Sleep(30 * time.Second)
		}
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		StartServer(Configuration)
	}()

	wg.Wait()
}
