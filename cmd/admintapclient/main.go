// admintapclient is a client that makes requests to the admin endpoint of Envoy, receives the stream, and outputs it to the log.

package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"

	"github.com/atpons/limelane/pkg/logutil"
	tapv2alpha "github.com/envoyproxy/go-control-plane/envoy/data/tap/v2alpha"
	"github.com/golang/protobuf/jsonpb"
	"go.uber.org/zap"
	"golang.org/x/net/http2"
)

type RequestBody struct {
	ConfigID  string     `json:"config_id"`
	TapConfig *TapConfig `json:"tap_config"`
}
type MatchConfig struct {
	AnyMatch bool `json:"any_match"`
}

type StreamingAdmin struct {
}

type Sinks struct {
	StreamingAdmin *StreamingAdmin `json:"streaming_admin"`
}

type OutputConfig struct {
	Sinks []*Sinks `json:"sinks"`
}

type TapConfig struct {
	MatchConfig  *MatchConfig  `json:"match_config"`
	OutputConfig *OutputConfig `json:"output_config"`
}

func NewRequestBody(configId string) *RequestBody {
	return &RequestBody{
		ConfigID: configId,
		TapConfig: &TapConfig{
			MatchConfig: &MatchConfig{AnyMatch: true},
			OutputConfig: &OutputConfig{Sinks: []*Sinks{
				{StreamingAdmin: &StreamingAdmin{}},
			}},
		},
	}
}

func main() {
	hcli := &http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			// force dial plain text
			DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
				return net.Dial(network, addr)
			},
		},
	}
	bi, err := json.Marshal(NewRequestBody(os.Getenv("CONFIG_ID")))
	if err != nil {
		panic(err)
	}
	req, err := http.NewRequest(
		http.MethodPost,
		"http://localhost:19000/tap",
		bytes.NewReader(bi),
	)
	if err != nil {
		panic(err)
	}
	resp, err := hcli.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	r := bufio.NewReader(resp.Body)
	b := make([]byte, 4096)
	for {
		l, err := r.Read(b)
		if err == io.EOF {
			fmt.Fprintf(os.Stderr, "eof")
		}
		if err != nil {
			break
		}
		if l > 0 {
			traceWrapper := tapv2alpha.TraceWrapper{}
			tdata := b[:l]
			if err := jsonpb.Unmarshal(bytes.NewReader(tdata), &traceWrapper); err != nil {
				fmt.Fprintf(os.Stderr, "%+v\n", err)
			}
			sbt := traceWrapper.GetSocketBufferedTrace()
			if sbt != nil {
				for _, v := range sbt.GetEvents() {
					if read := v.GetRead(); read != nil {
						logutil.L().Info("data read", zap.Uint64("trace_id", sbt.TraceId), zap.Int("read_bytes", len(v.GetRead().GetData().GetAsBytes())))
					}
					if write := v.GetWrite(); write != nil {
						logutil.L().Info("data write", zap.Uint64("trace_id", sbt.TraceId), zap.Int("write_bytes", len(v.GetWrite().GetData().GetAsBytes())))
					}
				}
			}
		}
	}
}
