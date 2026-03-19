package easyraft

import (
	"errors"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/netrusov/easyraft/discovery"
	"github.com/netrusov/easyraft/fsm"
	"github.com/netrusov/easyraft/serializer"
)

type Config struct {
	NodeID               string
	RaftPort             int
	DiscoveryPort        int
	DataDir              string
	Services             []fsm.FSMService
	Serializer           serializer.Serializer
	DiscoveryMethod      discovery.DiscoveryMethod
	SnapshotEnabled      bool
	Bootstrap            bool
	FormationTimeout     time.Duration
	ResolveAdvertiseAddr string
	Logger               hclog.Logger
}

const (
	defaultRaftPort         = 5000
	defaultDiscoveryPort    = 5001
	defaultDataDir          = "erdb"
	defaultFormationTimeout = 5 * time.Second
)

func DefaultConfig() *Config {
	return &Config{
		RaftPort:         defaultRaftPort,
		DiscoveryPort:    defaultDiscoveryPort,
		DataDir:          defaultDataDir,
		Services:         []fsm.FSMService{fsm.NewInMemoryMapService()},
		Serializer:       serializer.NewMsgPackSerializer(),
		Logger:           defaultLogger(),
		Bootstrap:        false,
		FormationTimeout: defaultFormationTimeout,
	}
}

func validateConfig(cfg *Config) error {
	if cfg == nil {
		return errors.New("config is required")
	}
	if cfg.Serializer == nil {
		return errors.New("serializer is required")
	}
	if cfg.DiscoveryMethod == nil {
		return errors.New("discovery method is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = defaultLogger()
	}

	return nil
}
