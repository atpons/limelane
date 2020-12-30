package main

import (
	"context"
	"errors"
	"os"

	"github.com/lifememoryteam/limeproxy/pkg/config"
	"github.com/lifememoryteam/limeproxy/pkg/repository"
	"github.com/lifememoryteam/limeproxy/pkg/server"
	"github.com/lifememoryteam/limeproxy/pkg/service"
	"github.com/lifememoryteam/limeproxy/pkg/xds"
)

func main() {
	if err := command(); err != nil {
		panic(err)
	}
}

func command() error {
	e := os.Getenv("CONFIG_FILE")
	if e == "" {
		return errors.New("not found")
	}
	c, err := config.NewFile(e)
	if err != nil {
		return err
	}
	r := repository.DummyRepository{}
	s := service.Build()
	x := xds.Build(context.Background(), &r, s)
	srv := server.Build(x)
	if err := srv.Run(c.GetConfig().Controlplane.ListenAddress); err != nil {
		return err
	}
	return nil
}
