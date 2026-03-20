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
	NodeID string

	AdvertiseAddr          string
	AdvertisePort          int
	AdvertiseAddrProbeHost string

	DiscoveryPort   int
	DiscoveryMethod discovery.DiscoveryMethod

	DataDir          string
	Services         []fsm.FSMService
	Serializer       serializer.Serializer
	SnapshotEnabled  bool
	Bootstrap        bool
	FormationTimeout time.Duration
	Logger           hclog.Logger
}

const (
	defaultAdvertisePort    = 5000
	defaultDiscoveryPort    = 5001
	defaultDataDir          = "erdb"
	defaultFormationTimeout = 5 * time.Second
)

func DefaultConfig() *Config {
	return &Config{
		AdvertisePort:    defaultAdvertisePort,
		DiscoveryPort:    defaultDiscoveryPort,
		DataDir:          defaultDataDir,
		Services:         []fsm.FSMService{fsm.NewInMemoryMapService()},
		Serializer:       serializer.NewMsgpackSerializer(),
		Logger:           defaultLogger(),
		Bootstrap:        false,
		FormationTimeout: defaultFormationTimeout,
	}
}

func validateConfig(cfg *Config) error {
	if cfg == nil {
		return errors.New("config is required")
	}
	if cfg.AdvertiseAddr == "" {
		cfg.AdvertiseAddr = "0.0.0.0"
	}
	if cfg.AdvertisePort == 0 {
		cfg.AdvertisePort = defaultAdvertisePort
	}
	if cfg.DiscoveryPort == 0 {
		cfg.DiscoveryPort = defaultDiscoveryPort
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
