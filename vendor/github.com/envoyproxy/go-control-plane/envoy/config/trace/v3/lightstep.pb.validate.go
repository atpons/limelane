// Code generated by protoc-gen-validate. DO NOT EDIT.
// source: envoy/config/trace/v3/lightstep.proto

package envoy_config_trace_v3

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"net/mail"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/golang/protobuf/ptypes"
)

// ensure the imports are used
var (
	_ = bytes.MinRead
	_ = errors.New("")
	_ = fmt.Print
	_ = utf8.UTFMax
	_ = (*regexp.Regexp)(nil)
	_ = (*strings.Reader)(nil)
	_ = net.IPv4len
	_ = time.Duration(0)
	_ = (*url.URL)(nil)
	_ = (*mail.Address)(nil)
	_ = ptypes.DynamicAny{}
)

// define the regex for a UUID once up-front
var _lightstep_uuidPattern = regexp.MustCompile("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")

// Validate checks the field values on LightstepConfig with the rules defined
// in the proto definition for this message. If any rules are violated, an
// error is returned.
func (m *LightstepConfig) Validate() error {
	if m == nil {
		return nil
	}

	if utf8.RuneCountInString(m.GetCollectorCluster()) < 1 {
		return LightstepConfigValidationError{
			field:  "CollectorCluster",
			reason: "value length must be at least 1 runes",
		}
	}

	if utf8.RuneCountInString(m.GetAccessTokenFile()) < 1 {
		return LightstepConfigValidationError{
			field:  "AccessTokenFile",
			reason: "value length must be at least 1 runes",
		}
	}

	for idx, item := range m.GetPropagationModes() {
		_, _ = idx, item

		if _, ok := LightstepConfig_PropagationMode_name[int32(item)]; !ok {
			return LightstepConfigValidationError{
				field:  fmt.Sprintf("PropagationModes[%v]", idx),
				reason: "value must be one of the defined enum values",
			}
		}

	}

	return nil
}

// LightstepConfigValidationError is the validation error returned by
// LightstepConfig.Validate if the designated constraints aren't met.
type LightstepConfigValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e LightstepConfigValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e LightstepConfigValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e LightstepConfigValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e LightstepConfigValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e LightstepConfigValidationError) ErrorName() string { return "LightstepConfigValidationError" }

// Error satisfies the builtin error interface
func (e LightstepConfigValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sLightstepConfig.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = LightstepConfigValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = LightstepConfigValidationError{}
