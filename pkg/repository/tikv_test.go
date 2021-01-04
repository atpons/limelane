package repository

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/atpons/limelane/pkg/model"

	"github.com/pingcap/tidb/store/mockstore"
)

func TestTiKVRepository_GetEndpoint(t *testing.T) {
	ctx := context.Background()

	storage, err := mockstore.NewMockStore()
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()

	repository := &TiKVRepository{storage: storage}

	want := &model.Endpoint{
		Name:       "testendpoint",
		Upstream:   "127.0.0.1",
		Port:       8690,
		ListenPort: 8691,
	}

	if err := repository.SetEndpoint(ctx, want); err != nil {
		t.Error(err)
	}

	got, err := repository.GetEndpoint(ctx, want.Name)
	if err != nil {
		t.Error(err)
	}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("GetEndpoint() mismatch (-want +got):\n%s", diff)
	}
}

func TestTiKVRepository_AllEndpoint(t *testing.T) {
	ctx := context.Background()

	storage, err := mockstore.NewMockStore()
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()

	repository := &TiKVRepository{storage: storage}

	want := map[string]*model.Endpoint{
		"testendpoint": {
			Name:       "testendpoint",
			Upstream:   "127.0.0.1",
			Port:       8690,
			ListenPort: 8691,
		},
		"testendpoint2": {
			Name:       "testendpoint2",
			Upstream:   "127.0.0.1",
			Port:       8690,
			ListenPort: 8692,
		},
	}

	for _, v := range want {
		if err := repository.SetEndpoint(ctx, v); err != nil {
			t.Error(err)
		}

	}

	gotSlice, err := repository.AllEndpoint(ctx)
	if err != nil {
		t.Error(err)
	}

	got := make(map[string]*model.Endpoint, 0)
	for _, v := range gotSlice {
		got[v.Name] = v
	}

	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("AllEndpoint() mismatch (-want +got):\n%s", diff)
	}
}
