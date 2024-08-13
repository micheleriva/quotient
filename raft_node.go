package main

import (
	"bytes"
	"compress/gzip"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"
)

type RaftNode struct {
	raft        *raft.Raft
	config      *Config
	fsm         *FSM
	transport   *raft.NetworkTransport
	logStore    *raftboltdb.BoltStore
	stableStore *raftboltdb.BoltStore
	snapshots   *raft.FileSnapshotStore
}

type RaftCommand struct {
	Operation string `json:"operation"`
	Key       string `json:"key"`
}

type FSM struct {
	qf *QuotientFilter
}

type FSMSnapshot struct {
	data []byte
}

func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd RaftCommand
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return fmt.Errorf("failed to unmarshal command: %v", err)
	}

	switch cmd.Operation {
	case "insert":
		return f.qf.Insert([]byte(cmd.Key))
	case "remove":
		return f.qf.Remove([]byte(cmd.Key))
	default:
		return fmt.Errorf("unknown command: %s", cmd.Operation)
	}
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	// Lock all stripes
	for i := 0; i < stripes; i++ {
		f.qf.locks[i].RLock()
	}
	defer func() {
		for i := 0; i < stripes; i++ {
			f.qf.locks[i].RUnlock()
		}
	}()

	var buf bytes.Buffer
	gzipWriter := gzip.NewWriter(&buf)
	enc := gob.NewEncoder(gzipWriter)

	err := enc.Encode(struct {
		Data     []uint64
		Mask     uint64
		Quotient uint
		Count    int64
	}{
		Data:     f.qf.data,
		Mask:     f.qf.mask,
		Quotient: f.qf.quotient,
		Count:    f.qf.count.Load(),
	})

	if err != nil {
		return nil, err
	}

	if err := gzipWriter.Close(); err != nil {
		return nil, err
	}

	return &FSMSnapshot{data: buf.Bytes()}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	gzipReader, err := gzip.NewReader(rc)
	if err != nil {
		return err
	}
	defer gzipReader.Close()

	dec := gob.NewDecoder(gzipReader)

	var snapshot struct {
		Data     []uint64
		Mask     uint64
		Quotient uint
		Count    int64
	}
	if err := dec.Decode(&snapshot); err != nil {
		return err
	}

	for i := 0; i < stripes; i++ {
		f.qf.locks[i].Lock()
	}
	defer func() {
		for i := 0; i < stripes; i++ {
			f.qf.locks[i].Unlock()
		}
	}()

	f.qf.data = snapshot.Data
	f.qf.mask = snapshot.Mask
	f.qf.quotient = snapshot.Quotient
	f.qf.count.Store(snapshot.Count)

	return nil
}

func (f *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	_, err := sink.Write(f.data)
	if err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (f *FSMSnapshot) Release() {}

func NewRaftNode(config *Config, qf *QuotientFilter) (*RaftNode, error) {
	fsm := &FSM{qf: qf}

	if err := os.MkdirAll(config.Raft.LogDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory: %v", err)
	}

	if err := os.MkdirAll(config.Raft.SnapshotDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create snapshot directory: %v", err)
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(config.Raft.LogDir, "raft-log.bolt"))
	if err != nil {
		return nil, fmt.Errorf("failed to create log store: %v", err)
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(config.Raft.LogDir, "raft-stable.bolt"))
	if err != nil {
		return nil, fmt.Errorf("failed to create stable store: %v", err)
	}

	snapshots, err := raft.NewFileSnapshotStore(config.Raft.SnapshotDir, 3, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot store: %v", err)
	}

	addr, err := net.ResolveTCPAddr("tcp", config.Raft.TCPAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve TCP address: %v", err)
	}

	transport, err := raft.NewTCPTransport(config.Raft.TCPAddress, addr, 3, config.Raft.Timeout, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create TCP transport: %v", err)
	}

	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(config.Raft.NodeID)
	raftConfig.HeartbeatTimeout = config.Raft.Timeout
	raftConfig.ElectionTimeout = config.Raft.Timeout * 2
	raftConfig.CommitTimeout = config.Raft.Timeout / 2
	raftConfig.MaxAppendEntries = 64
	raftConfig.ShutdownOnRemove = false

	r, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create new Raft: %v", err)
	}

	return &RaftNode{
		raft:        r,
		config:      config,
		fsm:         fsm,
		transport:   transport,
		logStore:    logStore,
		stableStore: stableStore,
		snapshots:   snapshots,
	}, nil
}

func (rn *RaftNode) Start() error {
	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(rn.config.Raft.NodeID),
				Address: rn.transport.LocalAddr(),
			},
		},
	}

	rn.raft.BootstrapCluster(configuration)
	return nil
}

func (rn *RaftNode) Stop() error {
	return rn.raft.Shutdown().Error()
}

func (rn *RaftNode) Insert(key string) error {
	cmd := RaftCommand{Operation: "insert", Key: key}
	return rn.applyCommand(cmd)
}

func (rn *RaftNode) Remove(key string) error {
	cmd := RaftCommand{Operation: "remove", Key: key}
	return rn.applyCommand(cmd)
}

func (rn *RaftNode) applyCommand(cmd RaftCommand) error {
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	future := rn.raft.Apply(data, 5*time.Second)
	if err := future.Error(); err != nil {
		return err
	}

	resp := future.Response()
	if err, ok := resp.(error); ok {
		return err
	}

	return nil
}

func (rn *RaftNode) AddPeer(nodeID, addr string) error {
	log.Printf("Adding peer: %s at %s", nodeID, addr)
	return rn.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0).Error()
}

func (rn *RaftNode) RemovePeer(nodeID string) error {
	log.Printf("Removing peer: %s", nodeID)
	return rn.raft.RemoveServer(raft.ServerID(nodeID), 0, 0).Error()
}

func (rn *RaftNode) IsLeader() bool {
	return rn.raft.State() == raft.Leader
}

func (rn *RaftNode) LeaderAddress() string {
	return string(rn.raft.Leader())
}
