package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/atpons/limelane/pkg/model"

	"github.com/gavv/httpexpect/v2"
	"github.com/atpons/limelane/pkg/logutil"
	"github.com/atpons/limelane/pkg/repository"
	"github.com/atpons/limelane/pkg/service"
	"github.com/atpons/limelane/pkg/xds"
)

type testServer struct {
	s      *Server
	server *httptest.Server
	e      *httpexpect.Expect
}

func (ts *testServer) Client() *httpexpect.Expect {
	return ts.e
}

func (ts *testServer) Close() error {
	ts.server.Close()
	return ts.s.repository.Close()
}

func Setup(t *testing.T) (*testServer, error) {
	t.Helper()
	repo, err := repository.NewMockRepository()
	if err != nil {
		return nil, err
	}
	xdsServer := xds.Build(context.Background(), repo, service.Build(), logutil.L())
	s := NewServer(repo, xdsServer)
	ts := httptest.NewServer(s.echo)
	e := httpexpect.WithConfig(httpexpect.Config{
		BaseURL:  ts.URL,
		Reporter: httpexpect.NewAssertReporter(t),
		Printers: []httpexpect.Printer{
			httpexpect.NewDebugPrinter(t, true),
		},
	})
	return &testServer{
		s:      s,
		server: ts,
		e:      e,
	}, nil
}

func TestServer_Register(t *testing.T) {
	ts, err := Setup(t)
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Close()
	t.Run("success", func(t *testing.T) {
		ts.Client().POST("/register").WithJSON(
			Snapshot{
				Version: "1",
				Endpoints: []*Endpoint{{
					Name:       "hoge",
					Upstream:   "127.0.0.1",
					Port:       8690,
					ListenPort: 28690,
				},
				},
			},
		).Expect().Status(http.StatusOK)
	})
}

func TestServer_Get(t *testing.T) {
	ts, err := Setup(t)
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Close()
	t.Run("success", func(t *testing.T) {
		want := &model.Endpoint{
			Name:       "testendpointget",
			Upstream:   "127.0.0.1",
			Port:       8690,
			ListenPort: 8691,
		}

		if err := ts.s.repository.SetEndpoint(context.Background(), want); err != nil {
			t.Fatal(err)
		}

		ts.Client().GET("/endpoint/testendpointget").Expect().Status(http.StatusOK).JSON().Object().Equal(
			&Endpoint{
				Name:       want.Name,
				Upstream:   want.Upstream,
				Port:       want.Port,
				ListenPort: want.ListenPort,
			},
		)
	})
}

func TestServer_Remove(t *testing.T) {
	ts, err := Setup(t)
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Close()
	t.Run("success", func(t *testing.T) {
		want := &model.Endpoint{
			Name:       "testendpointdelete",
			Upstream:   "127.0.0.1",
			Port:       8690,
			ListenPort: 8691,
		}

		if err := ts.s.repository.SetEndpoint(context.Background(), want); err != nil {
			t.Fatal(err)
		}

		ts.Client().DELETE("/endpoint/testendpointdelete").Expect().Status(http.StatusOK)

		if _, err := ts.s.repository.GetEndpoint(context.Background(), "testendpointdelete"); !errors.Is(err, repository.ErrNotFound) {
			t.Fatal(err)
		}
	})
}

func TestServer_Add(t *testing.T) {
	ts, err := Setup(t)
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Close()
	t.Run("success", func(t *testing.T) {
		want := &model.Endpoint{
			Name:       "testendpointadd",
			Upstream:   "127.0.0.1",
			Port:       8690,
			ListenPort: 8691,
		}

		ts.Client().POST("/endpoint").WithJSON(
			&Endpoint{
				Name:       want.Name,
				Upstream:   want.Upstream,
				Port:       want.Port,
				ListenPort: want.ListenPort,
			}).Expect().Status(http.StatusOK)
	})

	if _, err := ts.s.repository.GetEndpoint(context.Background(), "testendpointadd"); err != nil {
		t.Fatal(err)
	}
}

func TestServer_GetAll(t *testing.T) {
	ts, err := Setup(t)
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Close()
	t.Run("success", func(t *testing.T) {
		want := &model.Endpoint{
			Name:       "testendpointget",
			Upstream:   "127.0.0.1",
			Port:       8690,
			ListenPort: 8691,
		}

		want2 := &model.Endpoint{
			Name:       "testendpointget2",
			Upstream:   "127.0.0.1",
			Port:       8690,
			ListenPort: 8692,
		}

		if err := ts.s.repository.SetEndpoint(context.Background(), want); err != nil {
			t.Fatal(err)
		}

		if err := ts.s.repository.SetEndpoint(context.Background(), want2); err != nil {
			t.Fatal(err)
		}

		ts.Client().GET("/endpoint").Expect().Status(http.StatusOK).JSON().Array().Elements(
			&Endpoint{
				Name:       want.Name,
				Upstream:   want.Upstream,
				Port:       want.Port,
				ListenPort: want.ListenPort,
			},
			&Endpoint{
				Name:       want2.Name,
				Upstream:   want2.Upstream,
				Port:       want2.Port,
				ListenPort: want2.ListenPort,
			},
		)
	})
}
