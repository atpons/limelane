package repository

import "github.com/pingcap/tidb/store/mockstore"

type Repository interface {
	EndpointRepository
	Close() error
}

// for test by mock tikv
func NewMockRepository() (Repository, error) {
	storage, err := mockstore.NewMockStore()
	if err != nil {
		return nil, err
	}
	return &TiKVRepository{storage: storage}, nil
}
