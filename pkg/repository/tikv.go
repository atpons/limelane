package repository

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/atpons/limelane/pkg/model"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
)

var (
	ErrNotFound = errors.New("repository: data not found")
	ErrValidate = errors.New("repository: validate error")
)

type TiKVRepository struct {
	storage kv.Storage
}

func NewTiKVRepository(address string) (*TiKVRepository, error) {
	driver := tikv.Driver{}
	storage, err := driver.Open(address)
	if err != nil {
		return nil, err
	}
	repository := &TiKVRepository{storage: storage}
	return repository, nil
}

func (t *TiKVRepository) GetEndpoint(ctx context.Context, name string) (*model.Endpoint, error) {
	b, err := t.get(ctx, fmt.Sprintf("endpoint/%s", name))
	if err != nil {
		return nil, err
	}
	endpoint, err := model.UnmarshalEndpoint(b)
	if err != nil {
		return nil, err
	}
	return endpoint, nil
}

func (t *TiKVRepository) SetEndpoint(ctx context.Context, endpoint *model.Endpoint) error {
	b, err := model.MarshalEndpoint(endpoint)
	if err != nil {
		return err
	}
	kvs := map[string][]byte{
		fmt.Sprintf("endpoint/%s", endpoint.Name): b,
	}
	txn, err := t.storage.Begin()
	endpoints, err := t.getAllEndpoints(ctx, txn)
	if err != nil {
		return err
	}
	if err := t.validateEndpoint(endpoint, endpoints); err != nil {
		return err
	}
	if err := t.setMutation(ctx, txn, kvs); err != nil {
		return err
	}
	if err := txn.Commit(ctx); err != nil {
		return err
	}
	return nil
}

func (t *TiKVRepository) validateEndpoint(endpoint *model.Endpoint, existEndpoints []*model.Endpoint) error {
	for _, v := range existEndpoints {
		if endpoint.ListenPort == v.ListenPort {
			return ErrValidate
		}
	}
	return nil
}

func (t *TiKVRepository) AllEndpoint(ctx context.Context) ([]*model.Endpoint, error) {
	txn, err := t.storage.Begin()
	if err != nil {
		return nil, err
	}
	return t.getAllEndpoints(ctx, txn)
}

func (t *TiKVRepository) getAllEndpoints(ctx context.Context, txn kv.Transaction) ([]*model.Endpoint, error) {
	it, err := txn.Iter(kv.Key("endpoint/"), nil)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	endpoints := make([]*model.Endpoint, 0)
	for it.Valid() {
		endpoint, err := model.UnmarshalEndpoint(it.Value())
		if err != nil {
			return nil, err
		}
		endpoints = append(endpoints, endpoint)
		if err := it.Next(); err != nil {
			return nil, err
		}
	}
	return endpoints, nil
}

func (t *TiKVRepository) get(ctx context.Context, key string) ([]byte, error) {
	txn, err := t.storage.Begin()
	if err != nil {
		return nil, err
	}
	v, err := txn.Get(ctx, []byte(key))
	if errors.Is(err, kv.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (t *TiKVRepository) getAsUint32(ctx context.Context, key string) (uint32, error) {
	v, err := t.get(ctx, key)
	if err != nil {
		return 0, err
	}
	i, err := strconv.ParseUint(string(v), 10, 32)
	if err != nil {
		return 0, err
	}
	return uint32(i), nil

}

func (t *TiKVRepository) setMutation(ctx context.Context, txn kv.Transaction, kv map[string][]byte) error {
	for key, value := range kv {
		if err := txn.Set([]byte(key), value); err != nil {
			return err
		}
	}
	return nil
}

func (t *TiKVRepository) Close() error {
	return t.storage.Close()
}

func (t *TiKVRepository) RemoveEndpoint(ctx context.Context, name string) error {
	txn, err := t.storage.Begin()
	if err != nil {
		return err
	}
	if err := txn.Delete(kv.Key(fmt.Sprintf("endpoint/%s", name))); err != nil {
		return err
	}
	return txn.Commit(ctx)
}
