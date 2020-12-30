package service

import (
	"github.com/lifememoryteam/limeproxy/pkg/service/node"
	"github.com/lifememoryteam/limeproxy/pkg/service/snapshot"
)

type Services struct {
	Node *node.Service
	Snapshot *snapshot.Service
}

func Build() *Services {
	return &Services{
		Node:     &node.Service{},
		Snapshot: snapshot.NewSnapshot(),
	}
}
