package config

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Controlplane *Controlplane `yaml:"controlplane"`
	Admin        *Admin        `yaml:"admin"`
	Storage      *Storage      `yaml:"storage"`
}

type Controlplane struct {
	ListenAddress string `yaml:"listen_address"`
}

type Admin struct {
	ListenAddress string `yaml:"listen_address"`
}

type Storage struct {
	Type string `yaml:"type"`
	TiKV *TiKV  `yaml:"tikv"`
}

type TiKV struct {
	Address string `yaml:"address"`
}

type Source interface {
	GetConfig() *Config
}

type Static struct {
	c *Config
}

func NewStatic(c *Config) *Static {
	return &Static{c: c}
}

func (s *Static) GetConfig() *Config {
	return s.c
}

type File struct {
	path string
	c    *Config
}

func NewFile(path string) (*File, error) {
	c := Config{}
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	return &File{
		path: path,
		c:    &c,
	}, nil
}

func (f *File) GetConfig() *Config {
	return f.c
}
