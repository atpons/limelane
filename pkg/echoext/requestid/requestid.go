package requestid

import (
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
)

const (
	Key = "RequestId"
)

func Middleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		requestId := c.Get(Key)
		if requestId == "" {
			requestId = uuid.New().String()
		}
		c.Set(Key, requestId)
		return next(c)
	}
}

func Get(c echo.Context) string {
	requestId := c.Get(Key)
	if requestId == nil {
		requestId = uuid.New().String()
	}
	c.Set(Key, requestId)
	return requestId.(string)
}
