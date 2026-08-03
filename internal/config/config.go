package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the test configuration loaded from YAML.
type Config struct {
	Name      string            `yaml:"name"`
	Target    string            `yaml:"target"`
	Method    string            `yaml:"method"`
	Users     int               `yaml:"users"`
	Duration  string            `yaml:"duration"`
	RampUp    string            `yaml:"ramp_up"`
	ThinkTime string            `yaml:"think_time"`
	Headers   map[string]string `yaml:"headers"`
	Timeout   string            `yaml:"timeout"`
	Body      string            `yaml:"body"`
	Loops     int               `yaml:"loops"`
}

// LoadConfig reads and parses a YAML configuration file.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse yaml config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &cfg, nil
}

// Validate checks configuration values for correctness.
func (c *Config) Validate() error {
	if c.Name == "" {
		return errors.New("name is required")
	}

	if c.Target == "" {
		return errors.New("target URL is required")
	}
	u, err := url.ParseRequestURI(c.Target)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("target must be a valid HTTP or HTTPS URL: %s", c.Target)
	}

	c.Method = strings.ToUpper(c.Method)
	validMethods := map[string]bool{
		"GET": true, "POST": true, "PUT": true, "PATCH": true,
		"DELETE": true, "HEAD": true, "OPTIONS": true,
	}
	if !validMethods[c.Method] {
		return fmt.Errorf("unsupported HTTP method: %s", c.Method)
	}

	if c.Users <= 0 {
		return fmt.Errorf("users must be greater than 0: %d", c.Users)
	}

	if _, err := c.ParsedDuration(); err != nil {
		return fmt.Errorf("invalid duration: %w", err)
	}

	if _, err := c.ParsedRampUp(); err != nil {
		return fmt.Errorf("invalid ramp_up: %w", err)
	}

	if _, err := c.ParsedThinkTime(); err != nil {
		return fmt.Errorf("invalid think_time: %w", err)
	}

	if _, err := c.ParsedTimeout(); err != nil {
		return fmt.Errorf("invalid timeout: %w", err)
	}

	return nil
}

// ParsedDuration returns the test duration as time.Duration.
func (c *Config) ParsedDuration() (time.Duration, error) {
	if c.Duration == "" {
		return 0, errors.New("duration is empty")
	}
	return time.ParseDuration(c.Duration)
}

// ParsedRampUp returns the ramp-up time as time.Duration.
func (c *Config) ParsedRampUp() (time.Duration, error) {
	if c.RampUp == "" {
		return 0, nil // default to no ramp-up
	}
	return time.ParseDuration(c.RampUp)
}

// ParsedThinkTime returns the think-time as time.Duration.
func (c *Config) ParsedThinkTime() (time.Duration, error) {
	if c.ThinkTime == "" {
		return 0, nil // default to no think-time
	}
	return time.ParseDuration(c.ThinkTime)
}

// ParsedTimeout returns the request timeout as time.Duration.
func (c *Config) ParsedTimeout() (time.Duration, error) {
	if c.Timeout == "" {
		return 30 * time.Second, nil // default to 30s
	}
	return time.ParseDuration(c.Timeout)
}
