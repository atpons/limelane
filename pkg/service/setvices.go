package service

import (
	"github.com/atpons/limelane/pkg/service/node"
)

type Services struct {
	Node *node.Service
}

func Build() *Services {
	return &Services{
		Node: &node.Service{},
	}
}
