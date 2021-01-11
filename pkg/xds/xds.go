package xds

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/atpons/limelane/pkg/logutil"
	"github.com/atpons/limelane/pkg/repository"
	"github.com/atpons/limelane/pkg/service"
	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	matcher "github.com/envoyproxy/go-control-plane/envoy/config/common/matcher/v3"
	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpoint "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listener "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	tapconfigv3 "github.com/envoyproxy/go-control-plane/envoy/config/tap/v3"
	tapv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/common/tap/v3"
	tcpproxy "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/tcp_proxy/v3"
	tap "github.com/envoyproxy/go-control-plane/envoy/extensions/transport_sockets/tap/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	cachev3 "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	serverv3 "github.com/envoyproxy/go-control-plane/pkg/server/v3"
	testv3 "github.com/envoyproxy/go-control-plane/pkg/test/v3"
	"github.com/envoyproxy/go-control-plane/pkg/wellknown"
	"github.com/golang/protobuf/ptypes"
	"go.uber.org/zap"
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
		var l *Listener
		if v.Tap != nil {
			if v.Tap.Enabled && !v.Tap.IsAdminSinkType() {
				logutil.L().Info("tap enabled file", zap.String("type", v.Tap.SinkType))
				l, err = NewListener(v.Name, v.Name, v.ListenPort, TapAsFile())
				if err != nil {
					continue
				}
			}
			if v.Tap.Enabled && v.Tap.IsAdminSinkType() {
				logutil.L().Info("tap enabled admin", zap.String("type", v.Tap.SinkType))
				l, err = NewListener(v.Name, v.Name, v.ListenPort, TapAsStream())
				if err != nil {
					continue
				}
			}
			if !v.Tap.Enabled {
				logutil.L().Info("tap disabled")
				l, err = NewListener(v.Name, v.Name, v.ListenPort)
				if err != nil {
					continue
				}
			}
		} else {
			logutil.L().Info("tap disabled")
			l, err = NewListener(v.Name, v.Name, v.ListenPort)
			if err != nil {
				continue
			}
		}

		lst, err := BuildTCPListener(l)
		if err != nil {
			return err
		}
		listeners = append(listeners, lst)
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

type Listener struct {
	Name        string
	ClusterName string
	Port        uint32

	tapAsFile   bool
	tapAsStream bool
}

func (l *Listener) Validate() error {
	if l.tapAsFile && l.tapAsStream {
		return errors.New("xds: specifying must be sink file or stream")
	}
	return nil
}

type ListenerOption func(*Listener) error

func TapAsFile() ListenerOption {
	return func(l *Listener) error {
		l.tapAsFile = true
		return nil
	}
}

func TapAsStream() ListenerOption {
	return func(l *Listener) error {
		l.tapAsStream = true
		return nil
	}
}

func NewListener(listenerName, clusterName string, port uint32, option ...ListenerOption) (*Listener, error) {
	l := &Listener{
		Name:        listenerName,
		ClusterName: clusterName,
		Port:        port,
	}
	for _, o := range option {
		if err := o(l); err != nil {
			return nil, err
		}
	}
	if err := l.Validate(); err != nil {
		return nil, err
	}
	return l, nil
}

func BuildTCPListener(l *Listener) (*listener.Listener, error) {
	p := &tcpproxy.TcpProxy{
		StatPrefix:       fmt.Sprintf("tcp-%d", l.Port),
		ClusterSpecifier: &tcpproxy.TcpProxy_Cluster{Cluster: l.ClusterName},
	}
	pbst, err := ptypes.MarshalAny(p)
	if err != nil {
		panic(err)
	}
	var tapConf *tap.Tap
	if l.tapAsStream {
		tapConf = &tap.Tap{
			CommonConfig: &tapv3.CommonExtensionConfig{
				ConfigType: &tapv3.CommonExtensionConfig_AdminConfig{
					AdminConfig: &tapv3.AdminConfig{ConfigId: l.Name},
				},
			},
			TransportSocket: &core.TransportSocket{
				Name: wellknown.TransportSocketRawBuffer,
			},
		}
	}
	if l.tapAsFile {
		tapConf = &tap.Tap{
			CommonConfig: &tapv3.CommonExtensionConfig{
				ConfigType: &tapv3.CommonExtensionConfig_StaticConfig{
					StaticConfig: &tapconfigv3.TapConfig{
						Match: &matcher.MatchPredicate{
							Rule: &matcher.MatchPredicate_AnyMatch{AnyMatch: true},
						},
						OutputConfig: &tapconfigv3.OutputConfig{
							Sinks: []*tapconfigv3.OutputSink{
								//{
								//	Format: tapconfigv3.OutputSink_PROTO_BINARY,
								//	OutputSinkType: &tapconfigv3.OutputSink_StreamingAdmin{
								//		StreamingAdmin: &tapconfigv3.StreamingAdminSink{},
								//	},
								//},
								{
									Format:         tapconfigv3.OutputSink_PROTO_BINARY,
									OutputSinkType: &tapconfigv3.OutputSink_FilePerTap{FilePerTap: &tapconfigv3.FilePerTapSink{PathPrefix: fmt.Sprintf("/tmp/limelane_test_%s", l.Name)}},
								},
							},
						},
					},
				},
			},
			TransportSocket: &core.TransportSocket{
				Name: wellknown.TransportSocketRawBuffer,
			},
		}
	}
	var tpConfig *core.TransportSocket
	if tapConf != nil {
		pbtap, err := ptypes.MarshalAny(tapConf)
		if err != nil {
			return nil, err
		}
		tpConfig = &core.TransportSocket{
			Name:       wellknown.TransportSocketTap,
			ConfigType: &core.TransportSocket_TypedConfig{TypedConfig: pbtap},
		}
	}

	ls := &listener.Listener{
		Name: l.Name,
		Address: &core.Address{
			Address: &core.Address_SocketAddress{
				SocketAddress: &core.SocketAddress{
					Protocol: core.SocketAddress_TCP,
					Address:  "0.0.0.0",
					PortSpecifier: &core.SocketAddress_PortValue{
						PortValue: l.Port,
					},
				},
			},
		},
		FilterChains: []*listener.FilterChain{
			{
				TransportSocket: tpConfig,
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
	return ls, nil
}
