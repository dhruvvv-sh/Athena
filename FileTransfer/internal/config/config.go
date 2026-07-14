// Package config loads the FileTransfer configuration from a YAML file, applying
// environment-variable overrides and sensible defaults.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"gopkg.in/yaml.v3"
)

type Config struct {
	// Home is the deployment root that contains bin/ certs/ config/ lib/ logs/ tmp/.
	// Every relative path below is resolved against it. Not read from YAML — it is
	// derived from FT_HOME, else the binary's location (<home>/lib/<binary>), else CWD.
	Home string `yaml:"-"`

	Database Database `yaml:"database"`
	Master   Master   `yaml:"master"`
	Worker   Worker   `yaml:"worker"`
	S3       S3       `yaml:"s3"`
	Paths    Paths    `yaml:"paths"`
	TLS      TLS      `yaml:"tls"`
	Logging  Logging  `yaml:"logging"`
}

type Paths struct {
	TempDir  string `yaml:"temp_dir"`  // scratch space for chunk staging / assembly
	LogDir   string `yaml:"log_dir"`
	FlowsDir string `yaml:"flows_dir"` // dir of per-flow YAMLs
	AppsFile string `yaml:"apps_file"` // application name -> endpoint registry (applications.yml)
}

type TLS struct {
	Enabled  bool   `yaml:"enabled"`
	CertFile string `yaml:"cert_file"` // server public cert (PEM)
	KeyFile  string `yaml:"key_file"`  // server private key (PEM)
	CADir    string `yaml:"ca_dir"`    // trust store: dir of CA certs workers/clients trust

	// Mutual TLS: require and verify a client certificate on every connection. The
	// certificate's CN is matched against a flow (see internal/flows) to authenticate
	// and authorize the calling application.
	MTLS        bool   `yaml:"mtls"`
	ClientCADir string `yaml:"client_ca_dir"` // CAs used to verify client certs (default: ca_dir)
	ClientCert  string `yaml:"client_cert"`   // client cert this node presents (worker → master)
	ClientKey   string `yaml:"client_key"`
}

type Logging struct {
	ServerFile   string `yaml:"server_file"`   // server + error logs
	TransferFile string `yaml:"transfer_file"` // dedicated file-transfer audit log
	MaxSizeMB    int    `yaml:"max_size_mb"`   // rotate when a log exceeds this size
	MaxBackups   int    `yaml:"max_backups"`   // number of rotated files to keep
	MaxAgeDays   int    `yaml:"max_age_days"`  // max age of a rotated file
	Compress     bool   `yaml:"compress"`      // gzip rotated files
}

type Database struct {
	DSN string `yaml:"dsn"`
}

type Master struct {
	Addr      string `yaml:"addr"`
	ChunkSize int64  `yaml:"chunk_size"`
}

type Worker struct {
	MasterURL    string `yaml:"master_url"`
	PollInterval int    `yaml:"poll_interval"`
	Concurrency  int    `yaml:"concurrency"`
	NodeID       string `yaml:"node_id"`
}

type S3 struct {
	Region    string `yaml:"region"`
	Endpoint  string `yaml:"endpoint"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
}

// Load reads the YAML config at path (missing file is allowed — defaults apply),
// then applies environment overrides and defaults.
func Load(path string) (*Config, error) {
	c := &Config{}
	if b, err := os.ReadFile(path); err == nil {
		if err := yaml.Unmarshal(b, c); err != nil {
			return nil, fmt.Errorf("parse %s: %w", path, err)
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	// Env overrides.
	str := func(env string, dst *string) {
		if v := os.Getenv(env); v != "" {
			*dst = v
		}
	}
	i64 := func(env string, dst *int64) {
		if v := os.Getenv(env); v != "" {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				*dst = n
			}
		}
	}
	iVal := func(env string, dst *int) {
		if v := os.Getenv(env); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				*dst = n
			}
		}
	}
	str("FT_DATABASE_DSN", &c.Database.DSN)
	str("FT_MASTER_ADDR", &c.Master.Addr)
	i64("FT_CHUNK_SIZE", &c.Master.ChunkSize)
	str("FT_MASTER_URL", &c.Worker.MasterURL)
	iVal("FT_POLL_INTERVAL", &c.Worker.PollInterval)
	iVal("FT_CONCURRENCY", &c.Worker.Concurrency)
	str("FT_NODE_ID", &c.Worker.NodeID)
	str("FT_S3_REGION", &c.S3.Region)
	str("FT_S3_ENDPOINT", &c.S3.Endpoint)
	str("FT_S3_ACCESS_KEY", &c.S3.AccessKey)
	str("FT_S3_SECRET_KEY", &c.S3.SecretKey)
	str("FT_TEMP_DIR", &c.Paths.TempDir)
	str("FT_LOG_DIR", &c.Paths.LogDir)
	str("FT_APPS_FILE", &c.Paths.AppsFile)
	str("FT_TLS_CERT", &c.TLS.CertFile)
	str("FT_TLS_KEY", &c.TLS.KeyFile)
	str("FT_TLS_CA_DIR", &c.TLS.CADir)
	str("FT_TLS_CLIENT_CA_DIR", &c.TLS.ClientCADir)
	str("FT_TLS_CLIENT_CERT", &c.TLS.ClientCert)
	str("FT_TLS_CLIENT_KEY", &c.TLS.ClientKey)
	str("FT_FLOWS_DIR", &c.Paths.FlowsDir)
	if v := os.Getenv("FT_TLS_ENABLED"); v == "true" || v == "1" {
		c.TLS.Enabled = true
	}
	if v := os.Getenv("FT_TLS_MTLS"); v == "true" || v == "1" {
		c.TLS.MTLS = true
	}

	c.applyDefaults()
	// Resolve the Home directory, then make every configured path absolute against it.
	c.Home = resolveHome()
	rel := func(p *string) {
		if *p != "" && !filepath.IsAbs(*p) {
			*p = filepath.Join(c.Home, *p)
		}
	}
	rel(&c.Paths.TempDir)
	rel(&c.Paths.LogDir)
	rel(&c.Paths.FlowsDir)
	rel(&c.Paths.AppsFile)
	rel(&c.TLS.CertFile)
	rel(&c.TLS.KeyFile)
	rel(&c.TLS.CADir)
	rel(&c.TLS.ClientCADir)
	rel(&c.TLS.ClientCert)
	rel(&c.TLS.ClientKey)
	rel(&c.Logging.ServerFile)
	rel(&c.Logging.TransferFile)

	if c.Database.DSN == "" {
		return nil, fmt.Errorf("database.dsn is required (set it in the config or FT_DATABASE_DSN)")
	}
	return c, nil
}

// resolveHome finds the deployment root: FT_HOME, else the parent of the binary's
// directory (binary lives in <home>/lib/), else the current working directory.
func resolveHome() string {
	if h := os.Getenv("FT_HOME"); h != "" {
		if abs, err := filepath.Abs(h); err == nil {
			return abs
		}
		return h
	}
	if exe, err := os.Executable(); err == nil {
		// <home>/lib/filetransfer -> <home>
		if home := filepath.Dir(filepath.Dir(exe)); home != "" && home != "." {
			return home
		}
	}
	wd, _ := os.Getwd()
	return wd
}

func (c *Config) applyDefaults() {
	if c.Master.Addr == "" {
		c.Master.Addr = ":8088"
	}
	if c.Master.ChunkSize <= 0 {
		c.Master.ChunkSize = 8 << 20 // 8 MiB
	}
	if c.Worker.MasterURL == "" {
		c.Worker.MasterURL = "http://localhost:8088"
	}
	if c.Worker.PollInterval <= 0 {
		c.Worker.PollInterval = 3
	}
	if c.Worker.Concurrency <= 0 {
		c.Worker.Concurrency = 4
	}
	// Defaults are relative to Home (resolved in Load) — never reference an output/ dir.
	if c.Paths.TempDir == "" {
		c.Paths.TempDir = "tmp"
	}
	if c.Paths.LogDir == "" {
		c.Paths.LogDir = "logs"
	}
	if c.TLS.CertFile == "" {
		c.TLS.CertFile = "certs/server.crt"
	}
	if c.TLS.KeyFile == "" {
		c.TLS.KeyFile = "certs/server.key"
	}
	if c.TLS.CADir == "" {
		c.TLS.CADir = "certs/CAs"
	}
	if c.TLS.ClientCADir == "" {
		c.TLS.ClientCADir = c.TLS.CADir // verify client certs against the same trust store
	}
	if c.TLS.ClientCert == "" {
		c.TLS.ClientCert = "certs/client.crt"
	}
	if c.TLS.ClientKey == "" {
		c.TLS.ClientKey = "certs/client.key"
	}
	if c.Paths.FlowsDir == "" {
		c.Paths.FlowsDir = "flows"
	}
	if c.Paths.AppsFile == "" {
		c.Paths.AppsFile = "config/applications.yml"
	}
	if c.Logging.ServerFile == "" {
		c.Logging.ServerFile = filepath.Join(c.Paths.LogDir, "filetransfer.log")
	}
	if c.Logging.TransferFile == "" {
		c.Logging.TransferFile = filepath.Join(c.Paths.LogDir, "transfers.log")
	}
	if c.Logging.MaxSizeMB <= 0 {
		c.Logging.MaxSizeMB = 50
	}
	if c.Logging.MaxBackups <= 0 {
		c.Logging.MaxBackups = 10
	}
	if c.Logging.MaxAgeDays <= 0 {
		c.Logging.MaxAgeDays = 30
	}
}
