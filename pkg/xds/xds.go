package xds

import (
	"context"
	cachev3 "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	serverv3 "github.com/envoyproxy/go-control-plane/pkg/server/v3"
	testv3 "github.com/envoyproxy/go-control-plane/pkg/test/v3"
	"github.com/lifememoryteam/limeproxy/pkg/repository"
	"github.com/lifememoryteam/limeproxy/pkg/service"
)

type XDS struct {
	Cache *cachev3.SnapshotCache
	Server serverv3.Server
	Repository repository.Repository
	Service *service.Services
}

func Build(ctx context.Context, repository repository.Repository, services *service.Services) *XDS {
	snapshot := cachev3.NewSnapshotCache(false, cachev3.IDHash{}, nil)
	s := serverv3.NewServer(ctx, snapshot, &testv3.Callbacks{Debug: true})
	return &XDS{Server: s, Cache: &snapshot, Repository: repository, Service: services}
}
