package main

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Quotient struct {
		LogSize uint `yaml:"logSize"`
	}

	Server struct {
		Port        int    `yaml:"port"`
		Concurrency int    `yaml:"concurrency"`
		APIKey      string `yaml:"api_key"`
	} `yaml:"server"`

	Raft struct {
		NodeID      string        `yaml:"node_id"`
		TCPAddress  string        `yaml:"tcp_address"`
		Timeout     time.Duration `yaml:"timeout"`
		SnapshotDir string        `yaml:"snapshot_dir"`
		LogDir      string        `yaml:"log_dir"`
	} `yaml:"raft"`
}

const (
	DefaultConfigFilename = "quotient.config.yaml"
	defaultServerPort     = 8080
	defaultAPIKey         = "xyz"
	defaultSnapshotDir    = "/quotient/raft/snapshots"
	defaultLogDir         = "/quotient/raft/logs"
	defaultLogSize        = 22
)

func createDefaultConfig() *Config {
	return &Config{
		Quotient: struct {
			LogSize uint `yaml:"logSize"`
		}{
			LogSize: defaultLogSize,
		},

		Server: struct {
			Port        int    `yaml:"port"`
			Concurrency int    `yaml:"concurrency"`
			APIKey      string `yaml:"api_key"`
		}{
			Port:        defaultServerPort,
			Concurrency: runtime.NumCPU(),
			APIKey:      defaultAPIKey,
		},

		Raft: struct {
			NodeID      string        `yaml:"node_id"`
			TCPAddress  string        `yaml:"tcp_address"`
			Timeout     time.Duration `yaml:"timeout"`
			SnapshotDir string        `yaml:"snapshot_dir"`
			LogDir      string        `yaml:"log_dir"`
		}{
			NodeID:      GenerateUUID(),
			TCPAddress:  fmt.Sprintf("0.0.0.0:%d", defaultServerPort),
			Timeout:     10 * time.Second,
			SnapshotDir: defaultSnapshotDir,
			LogDir:      defaultLogDir,
		},
	}
}

func mergeConfigs(defaultConfig, userConfig Config) Config {
	mergedConfig := defaultConfig

	if userConfig.Quotient.LogSize > 0 {
		mergedConfig.Quotient.LogSize = userConfig.Quotient.LogSize
	}
	if userConfig.Server.Port != 0 {
		mergedConfig.Server.Port = userConfig.Server.Port
	}
	if userConfig.Server.Concurrency != 0 {
		mergedConfig.Server.Concurrency = userConfig.Server.Concurrency
	}
	if userConfig.Server.APIKey != "" {
		mergedConfig.Server.APIKey = userConfig.Server.APIKey
	}
	if userConfig.Raft.NodeID != "" {
		mergedConfig.Raft.NodeID = userConfig.Raft.NodeID
	}
	if userConfig.Raft.TCPAddress != "" {
		mergedConfig.Raft.TCPAddress = userConfig.Raft.TCPAddress
	}
	if userConfig.Raft.Timeout != 0 {
		mergedConfig.Raft.Timeout = userConfig.Raft.Timeout
	}
	if userConfig.Raft.SnapshotDir != "" {
		mergedConfig.Raft.SnapshotDir = userConfig.Raft.SnapshotDir
	}
	if userConfig.Raft.LogDir != "" {
		mergedConfig.Raft.LogDir = userConfig.Raft.LogDir
	}

	return mergedConfig
}

func ParseConfigFile(filename string) (*Config, error) {
	if filename == "" {
		filename = DefaultConfigFilename
	}

	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("could not open config file: %w", err)
	}
	defer file.Close()

	userConfig := &Config{}
	decoder := yaml.NewDecoder(file)
	if err := decoder.Decode(userConfig); err != nil {
		return nil, fmt.Errorf("could not decode config file: %w", err)
	}

	defaultConfig := createDefaultConfig()
	finalConfig := mergeConfigs(*defaultConfig, *userConfig)

	return &finalConfig, nil
}
