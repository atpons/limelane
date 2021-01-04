package repository

import (
	"context"
	"errors"

	"github.com/atpons/limelane/pkg/model"
)

type EndpointRepository interface {
	GetEndpoint(ctx context.Context, name string) (*model.Endpoint, error)
	SetEndpoint(ctx context.Context, endpoint *model.Endpoint) error
	AllEndpoint(ctx context.Context) ([]*model.Endpoint, error)
	RemoveEndpoint(ctx context.Context, name string) error
}

func (d *DummyRepository) GetEndpoint(ctx context.Context, name string) (*model.Endpoint, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if v, ok := d.endpoints[name]; !ok {
		return nil, errors.New("not found")
	} else {
		return v, nil
	}
}

func (d *DummyRepository) SetEndpoint(ctx context.Context, endpoint *model.Endpoint) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.endpoints[endpoint.Name] = endpoint
	return nil
}

func (d *DummyRepository) AllEndpoint(ctx context.Context) ([]*model.Endpoint, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	e := make([]*model.Endpoint, 0)
	for _, v := range d.endpoints {
		e = append(e, v)
	}
	return e, nil
}

func (d *DummyRepository) RemoveEndpoint(ctx context.Context, name string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.endpoints, name)
	return nil
}
