package server

import (
	"net"

	"github.com/atpons/limelane/pkg/xds"
	clusterservice "github.com/envoyproxy/go-control-plane/envoy/service/cluster/v3"
	discoverygrpc "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	endpointservice "github.com/envoyproxy/go-control-plane/envoy/service/endpoint/v3"
	listenerservice "github.com/envoyproxy/go-control-plane/envoy/service/listener/v3"
	routeservice "github.com/envoyproxy/go-control-plane/envoy/service/route/v3"
	runtimeservice "github.com/envoyproxy/go-control-plane/envoy/service/runtime/v3"
	secretservice "github.com/envoyproxy/go-control-plane/envoy/service/secret/v3"
	serverv3 "github.com/envoyproxy/go-control-plane/pkg/server/v3"
	"google.golang.org/grpc"
)

const (
	grpcMaxConcurrentStreams = 1000000
)

func registerServer(grpcServer *grpc.Server, server serverv3.Server) {
	discoverygrpc.RegisterAggregatedDiscoveryServiceServer(grpcServer, server)
	endpointservice.RegisterEndpointDiscoveryServiceServer(grpcServer, server)
	clusterservice.RegisterClusterDiscoveryServiceServer(grpcServer, server)
	routeservice.RegisterRouteDiscoveryServiceServer(grpcServer, server)
	listenerservice.RegisterListenerDiscoveryServiceServer(grpcServer, server)
	secretservice.RegisterSecretDiscoveryServiceServer(grpcServer, server)
	runtimeservice.RegisterRuntimeDiscoveryServiceServer(grpcServer, server)
}

type Server struct {
	Server *grpc.Server
	XDS    *xds.XDS
}

func Build(xds *xds.XDS) *Server {
	var grpcOptions []grpc.ServerOption
	grpcOptions = append(grpcOptions, grpc.MaxConcurrentStreams(grpcMaxConcurrentStreams))
	grpcServer := grpc.NewServer(grpcOptions...)

	registerServer(grpcServer, xds.Server)

	return &Server{
		Server: grpcServer,
		XDS:    xds,
	}
}

func (s *Server) Run(addr string) error {
	l, err := net.Listen("tcp4", addr)
	if err != nil {
		return err
	}
	if err := s.Server.Serve(l); err != nil {
		return err
	}
	return nil
}

func (s *Server) Close() {
	s.Server.GracefulStop()
}
