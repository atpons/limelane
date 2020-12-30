package repository

import (
	"errors"
	"fmt"
	"github.com/lifememoryteam/limeproxy/pkg/model"
	"sync"
)

type NodeRepository interface {
	GetNode(node string) (*model.Node, error)
	SetNode(node *model.Node) error
}

type DummyRepository struct {
	mu *sync.RWMutex
	nodes []*model.Node
}

func (d *DummyRepository) searchNode(node string) *model.Node {
	d.mu.RLock()
	defer d.mu.RUnlock()

	for _, v := range d.nodes {
		if node == fmt.Sprintf("%s/%s", v.Cluster, v.ID) {
			return v
		}
	}

	return nil
}

func (d *DummyRepository) GetNode(node string) (*model.Node, error) {
	n := d.searchNode(node)
	if n != nil {
		return n, nil
	}
	return nil, errors.New("not found")
}

func (d *DummyRepository) SetNode(node *model.Node) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.nodes = append(d.nodes, node)
	return nil
}
