package model

import "gopkg.in/yaml.v2"

type Endpoint struct {
	Name       string `yaml:"name"`
	Upstream   string `yaml:"upstream"`
	Port       uint32 `yaml:"port"`
	ListenPort uint32 `yaml:"listen_port"`
}

func MarshalEndpoint(e *Endpoint) ([]byte, error) {
	b, err := yaml.Marshal(e)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func UnmarshalEndpoint(b []byte) (*Endpoint, error) {
	e := Endpoint{}
	if err := yaml.Unmarshal(b, &e); err != nil {
		return nil, err
	}
	return &e, nil
}
