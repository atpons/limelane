package xds

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/atpons/limelane/pkg/logutil"

	"github.com/envoyproxy/go-control-plane/pkg/cache/types"

	"github.com/golang/protobuf/ptypes"

	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpoint "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listener "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	tcpproxy "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/tcp_proxy/v3"

	cachev3 "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	serverv3 "github.com/envoyproxy/go-control-plane/pkg/server/v3"
	testv3 "github.com/envoyproxy/go-control-plane/pkg/test/v3"
	"github.com/envoyproxy/go-control-plane/pkg/wellknown"
	"github.com/atpons/limelane/pkg/repository"
	"github.com/atpons/limelane/pkg/service"
)

type XDS struct {
	version    *Version
	Snapshot   cachev3.SnapshotCache
	Server     serverv3.Server
	Repository repository.Repository
	Service    *service.Services
}

type Version struct {
	mu      sync.RWMutex
	version int64
}

func (v *Version) Inc() string {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.version = time.Now().Unix()
	return fmt.Sprint(v.version)
}

func (v *Version) Get() string {
	v.mu.RUnlock()
	defer v.mu.RUnlock()
	return fmt.Sprint(v.version)
}

func Build(ctx context.Context, repository repository.Repository, services *service.Services, logger *zap.Logger) *XDS {
	envoyLogger := logutil.NewEnvoyLogger(logger)
	snapshot := cachev3.NewSnapshotCache(false, cachev3.IDHash{}, envoyLogger)
	s := serverv3.NewServer(ctx, snapshot, &testv3.Callbacks{Debug: true})
	v := &Version{
		mu:      sync.RWMutex{},
		version: time.Now().Unix(),
	}
	return &XDS{version: v, Server: s, Repository: repository, Service: services, Snapshot: snapshot}
}

func (x *XDS) Sync() error {
	var clusters, listeners []types.Resource

	e, err := x.Repository.AllEndpoint(context.Background())
	if err != nil {
		return err
	}
	for _, v := range e {
		clusters = append(clusters, makeCluster(v.Name, v.Upstream, v.Port))
		listeners = append(listeners, makeTCPListener(v.Name, v.Name, v.ListenPort))
	}

	s := cachev3.NewSnapshot(
		x.version.Inc(),
		[]types.Resource{}, // endpoints
		clusters,
		[]types.Resource{}, // endpoints
		listeners,
		[]types.Resource{}, // runtimes
		[]types.Resource{}, // secrets
	)

	if err := s.Consistent(); err != nil {
		logutil.L().Error("consistency check failed", zap.Error(err))
		return err
	}

	if err := x.Snapshot.SetSnapshot("test-id", s); err != nil {
		logutil.L().Error("set snapshot failed", zap.Error(err))
		return err
	}

	return nil
}

func makeCluster(clusterName, upstream string, port uint32) *cluster.Cluster {
	return &cluster.Cluster{
		Name:                 clusterName,
		ConnectTimeout:       ptypes.DurationProto(5 * time.Second),
		ClusterDiscoveryType: &cluster.Cluster_Type{Type: cluster.Cluster_LOGICAL_DNS},
		LbPolicy:             cluster.Cluster_ROUND_ROBIN,
		LoadAssignment:       makeEndpoint(clusterName, upstream, port),
		DnsLookupFamily:      cluster.Cluster_V4_ONLY,
	}
}

func makeEndpoint(clusterName string, upstream string, port uint32) *endpoint.ClusterLoadAssignment {
	return &endpoint.ClusterLoadAssignment{
		ClusterName: clusterName,
		Endpoints: []*endpoint.LocalityLbEndpoints{{
			LbEndpoints: []*endpoint.LbEndpoint{{
				HostIdentifier: &endpoint.LbEndpoint_Endpoint{
					Endpoint: &endpoint.Endpoint{
						Address: &core.Address{
							Address: &core.Address_SocketAddress{
								SocketAddress: &core.SocketAddress{
									Protocol: core.SocketAddress_TCP,
									Address:  upstream,
									PortSpecifier: &core.SocketAddress_PortValue{
										PortValue: port,
									},
								},
							},
						},
					},
				},
			}},
		}},
	}
}

func makeTCPListener(listenerName, clusterName string, port uint32) *listener.Listener {
	p := &tcpproxy.TcpProxy{
		StatPrefix:       fmt.Sprintf("tcp-%d", port),
		ClusterSpecifier: &tcpproxy.TcpProxy_Cluster{Cluster: clusterName},
	}
	pbst, err := ptypes.MarshalAny(p)
	if err != nil {
		panic(err)
	}

	l := &listener.Listener{
		Name: listenerName,
		Address: &core.Address{
			Address: &core.Address_SocketAddress{
				SocketAddress: &core.SocketAddress{
					Protocol: core.SocketAddress_TCP,
					Address:  "0.0.0.0",
					PortSpecifier: &core.SocketAddress_PortValue{
						PortValue: port,
					},
				},
			},
		},
		FilterChains: []*listener.FilterChain{
			{
				Filters: []*listener.Filter{
					{
						Name: wellknown.TCPProxy,
						ConfigType: &listener.Filter_TypedConfig{
							TypedConfig: pbst,
						},
					},
				},
			},
		},
	}

	return l
}
