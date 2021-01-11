package exception

import (
	"fmt"
	"net/http"

	"github.com/atpons/limelane/pkg/echoext/requestid"
	"github.com/atpons/limelane/pkg/logutil"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

type InternalError struct {
	stacktrace string
	Err        error
}

func (ie *InternalError) Error() string {
	return ie.Err.Error()
}

func InternalServerError(err error) error {
	return &InternalError{
		stacktrace: fmt.Sprintf("%+v", err),
		Err:        err,
	}
}

func ErrorHandler() echo.HTTPErrorHandler {
	return func(err error, e echo.Context) {
		code := http.StatusInternalServerError
		var body interface{}
		body = echo.Map{"message": http.StatusText(code)}
		if he, ok := err.(*echo.HTTPError); ok {
			code = he.Code
			switch m := he.Message.(type) {
			case error:
				body = echo.Map{"message": m.Error()}
			default:
				body = echo.Map{"message": m}
			}
		}
		if ie, ok := err.(*InternalError); ok {
			logutil.L().Error("internal error", zap.Error(ie), zap.String("caller_stacktrace", ie.stacktrace), zap.String("request_id", requestid.Get(e)))
		}
		e.JSON(code, body)
	}
}
