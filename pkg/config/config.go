package config

import (
	"io/ioutil"
	"log"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Controlplane *Controlplane `yaml:"controlplane"`
}

type Controlplane struct {
	ListenAddress string `yaml:"listen_address"`
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
	log.Println(f.c)
	return f.c
}
