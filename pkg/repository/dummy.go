package repository

import (
	"sync"

	"github.com/atpons/limelane/pkg/model"
)

type DummyRepository struct {
	mu        sync.RWMutex
	endpoints map[string]*model.Endpoint
}

func NewDummyRepository() *DummyRepository {
	d := &DummyRepository{
		endpoints: make(map[string]*model.Endpoint, 0),
	}

	return d
}

func (d *DummyRepository) Close() error {
	return nil
}
