package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"go.uber.org/zap"

	"github.com/atpons/limelane/pkg/logutil"

	adminServer "github.com/atpons/limelane/pkg/admin/server"
	"github.com/atpons/limelane/pkg/config"
	"github.com/atpons/limelane/pkg/repository"
	"github.com/atpons/limelane/pkg/server"
	"github.com/atpons/limelane/pkg/service"
	"github.com/atpons/limelane/pkg/xds"
)

func main() {
	if err := command(); err != nil {
		panic(err)
	}
}

var (
	sv *server.Server
	as *adminServer.Server
)

func command() error {
	e := os.Getenv("CONFIG_FILE")
	if e == "" {
		return errors.New("not found")
	}
	c, err := config.NewFile(e)
	if err != nil {
		return err
	}
	var r repository.Repository
	switch c.GetConfig().Storage.Type {
	case "dummy":
		logutil.L().Warn("running on dummy storage")
		r = repository.NewDummyRepository()
	case "tikv":
		repo, err := repository.NewTiKVRepository(c.GetConfig().Storage.TiKV.Address)
		if err != nil {
			return err
		}
		r = repo
	}
	s := service.Build()
	x := xds.Build(context.Background(), r, s, logutil.L())
	sv = server.Build(x)
	as = adminServer.NewServer(r, x)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		if err := as.Run(c.GetConfig().Admin.ListenAddress); err != nil {
			fmt.Fprintf(os.Stderr, "%+v", err)
		}
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		if err := sv.Run(c.GetConfig().Controlplane.ListenAddress); err != nil {
			fmt.Fprintf(os.Stderr, "%+v", err)
		}
		wg.Done()
	}()
	wg.Add(1)
	cChan := make(chan os.Signal, 1)
	signal.Notify(cChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGHUP)
	go func() {
		<-cChan
		logutil.L().Info("start shutdown")
		sv.Close()
		if err := as.Close(); err != nil {
			logutil.L().Error("stop admin server error", zap.Error(err))
		}
		r.Close()
		wg.Done()
	}()
	wg.Wait()
	return nil
}
