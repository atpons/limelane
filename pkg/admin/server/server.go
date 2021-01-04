package server

import (
	"context"
	"net/http"

	"github.com/atpons/limelane/pkg/echoext/logging"

	"github.com/pkg/errors"

	"github.com/atpons/limelane/pkg/echoext/requestid"

	"github.com/atpons/limelane/pkg/echoext/exception"

	"github.com/labstack/echo/v4"
	"github.com/atpons/limelane/pkg/model"
	"github.com/atpons/limelane/pkg/repository"
	"github.com/atpons/limelane/pkg/xds"
)

type Server struct {
	echo *echo.Echo

	repository repository.Repository
	xds        *xds.XDS
}

type Snapshot struct {
	Version   string      `json:"-"`
	Endpoints []*Endpoint `json:"endpoints"`
}

type Endpoint struct {
	Name       string `json:"name"`
	Upstream   string `json:"upstream"`
	Port       uint32 `json:"port"`
	ListenPort uint32 `json:"listen_port"`
}

func NewServer(repository repository.Repository, xds *xds.XDS) *Server {
	e := echo.New()

	s := &Server{
		echo:       e,
		repository: repository,
		xds:        xds,
	}

	e.Use(logging.Middleware)
	e.Use(requestid.Middleware)

	e.HTTPErrorHandler = exception.ErrorHandler()

	e.POST("/register", s.Register)
	e.GET("/endpoint/:name", s.Get)
	e.DELETE("/endpoint/:name", s.Remove)
	e.POST("/endpoint", s.Add)
	e.GET("/endpoint", s.GetAll)

	return s
}

func (h *Server) Run(address string) error {
	return h.echo.Start(address)
}

func (h *Server) Close() error {
	return h.echo.Close()
}

func (h *Server) Register(c echo.Context) error {
	s := &Snapshot{}
	if err := c.Bind(s); err != nil {
		return exception.InternalServerError(errors.WithStack(err))
	}

	for _, v := range s.Endpoints {
		if err := h.repository.SetEndpoint(context.Background(), &model.Endpoint{
			Name:       v.Name,
			Upstream:   v.Upstream,
			Port:       v.Port,
			ListenPort: v.ListenPort,
		}); err != nil {
			return exception.InternalServerError(errors.WithStack(err))
		}
	}

	if err := h.xds.Sync(); err != nil {
		return exception.InternalServerError(errors.WithStack(err))
	}

	return c.JSON(http.StatusOK, nil)
}

func (h *Server) Get(c echo.Context) error {
	endpoint, err := h.repository.GetEndpoint(c.Request().Context(), c.Param("name"))
	if errors.Is(err, repository.ErrNotFound) {
		return c.NoContent(http.StatusNotFound)
	}
	if err != nil {
		return exception.InternalServerError(errors.WithStack(err))
	}
	return c.JSON(http.StatusOK, &Endpoint{
		Name:       endpoint.Name,
		Upstream:   endpoint.Upstream,
		Port:       endpoint.Port,
		ListenPort: endpoint.ListenPort,
	})
}

func (h *Server) Remove(c echo.Context) error {
	if err := h.repository.RemoveEndpoint(c.Request().Context(), c.Param("name")); err != nil {
		return exception.InternalServerError(errors.WithStack(err))
	}

	if err := h.xds.Sync(); err != nil {
		return exception.InternalServerError(errors.WithStack(err))
	}

	return c.JSON(http.StatusOK, nil)
}

func (h *Server) Add(c echo.Context) error {
	var s Endpoint

	if err := c.Bind(&s); err != nil {
		return exception.InternalServerError(errors.WithStack(err))
	}

	if err := h.repository.SetEndpoint(context.Background(), &model.Endpoint{
		Name:       s.Name,
		Upstream:   s.Upstream,
		Port:       s.Port,
		ListenPort: s.ListenPort,
	}); err != nil {
		return exception.InternalServerError(errors.WithStack(err))
	}

	if err := h.xds.Sync(); err != nil {
		return exception.InternalServerError(errors.WithStack(err))
	}

	return c.JSON(http.StatusOK, nil)
}

func (h *Server) GetAll(c echo.Context) error {
	var s []*Endpoint

	endpoints, err := h.repository.AllEndpoint(c.Request().Context())
	if err != nil {
		return exception.InternalServerError(errors.WithStack(err))
	}
	for _, v := range endpoints {
		s = append(s, &Endpoint{
			Name:       v.Name,
			Upstream:   v.Upstream,
			Port:       v.Port,
			ListenPort: v.ListenPort,
		})
	}

	return c.JSON(http.StatusOK, s)
}

func (h *Server) Error(c echo.Context) error {
	err := errors.New("test error")
	return exception.InternalServerError(errors.WithStack(err))
}
