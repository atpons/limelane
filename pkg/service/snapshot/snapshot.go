package snapshot

import cachev3 "github.com/envoyproxy/go-control-plane/pkg/cache/v3"

type Service struct {
	snapshotCache cachev3.SnapshotCache
}

func NewSnapshot() *Service {
	cache := cachev3.NewSnapshotCache(false, cachev3.IDHash{}, nil)

	return &Service{snapshotCache: cache}
}
