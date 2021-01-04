package logging

import (
	"fmt"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/atpons/limelane/pkg/echoext/requestid"
	"github.com/atpons/limelane/pkg/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func Middleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		start := time.Now()

		err := next(c)
		if err != nil {
			c.Error(err)
		}

		req := c.Request()
		res := c.Response()

		fields := []zapcore.Field{
			zap.String("remote_ip", c.RealIP()),
			zap.String("time", time.Since(start).String()),
			zap.String("host", req.Host),
			zap.String("request", fmt.Sprintf("%s %s", req.Method, req.RequestURI)),
			zap.Int("status", res.Status),
			zap.Int64("size", res.Size),
			zap.String("user_agent", req.UserAgent()),
		}

		fields = append(fields, zap.String("request_id", requestid.Get(c)))

		n := res.Status
		switch {
		case n >= 500:
			logutil.L().Error("Server error", fields...)
		case n >= 400:
			logutil.L().Warn("Client error", fields...)
		case n >= 300:
			logutil.L().Info("Redirection", fields...)
		default:
			logutil.L().Info("Success", fields...)
		}

		return nil
	}
}
