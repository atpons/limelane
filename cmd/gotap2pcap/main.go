// gotap2pcap uses pcapgo to convert Envoy's tap trace Protocol Buffer file into a PCAP file.
// This is a reimplementation of tap2pcap.py.
// ref: https://github.com/envoyproxy/envoy/blob/master/api/tools/tap2pcap.py

package main

import (
	"io"
	"io/ioutil"
	"net"
	"os"
	"time"

	"github.com/atpons/limelane/pkg/logutil"
	tapv2alpha "github.com/envoyproxy/go-control-plane/envoy/data/tap/v2alpha"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcapgo"
	"go.uber.org/zap"
)

type TCPWriter struct {
	w   *pcapgo.Writer
	Seq uint32
	Ack uint32
}

func NewWriter(writer io.Writer) (*TCPWriter, error) {
	w := &TCPWriter{
		w:   pcapgo.NewWriter(writer),
		Seq: 0,
		Ack: 0,
	}
	if err := w.w.WriteFileHeader(65536, layers.LinkTypeIPv4); err != nil {
		return nil, err
	}
	return w, nil
}

type LayerInfo struct {
	SrcPort layers.TCPPort
	DstPort layers.TCPPort
	SrcIP   net.IP
	DstIP   net.IP
}

func (w *TCPWriter) WritePacket(t time.Time, l *LayerInfo, b []byte, write bool) error {
	buf := gopacket.NewSerializeBuffer()
	err := gopacket.SerializeLayers(buf, gopacket.SerializeOptions{FixLengths: true}, &layers.IPv4{
		Version:  4,
		TTL:      64,
		SrcIP:    l.SrcIP,
		DstIP:    l.DstIP,
		Protocol: layers.IPProtocolTCP,
	}, &layers.TCP{
		Window:  8192,
		SrcPort: l.SrcPort,
		DstPort: l.DstPort,
		SYN:     false,
		ACK:     true,
		PSH:     false,
		Seq:     w.Seq,
		Ack:     w.Ack,
	},
		gopacket.Payload(b),
	)
	if err != nil {
		return err
	}
	captureInfo := gopacket.CaptureInfo{
		Timestamp:      t,
		CaptureLength:  len(buf.Bytes()),
		Length:         len(buf.Bytes()),
		InterfaceIndex: 0,
		AncillaryData:  nil,
	}
	if !write {
		w.Ack += uint32(len(b))
	} else {
		w.Seq += w.Ack
	}
	return w.w.WritePacket(captureInfo, buf.Bytes())
}

func main() {
	from := os.Getenv("FROM_FILE")
	to := os.Getenv("TO_PCAP")

	logutil.L().Info("starting transition", zap.String("from", from), zap.String("to", to))

	traceWrapper := tapv2alpha.TraceWrapper{}
	b, err := ioutil.ReadFile(from)
	if err != nil {
		panic(err)
	}

	if err := proto.Unmarshal(b, &traceWrapper); err != nil {
		panic(err)
	}

	trace := traceWrapper.GetSocketBufferedTrace()
	if trace == nil {
		logutil.L().Info("not found socket buffered trace, exiting")
		return
	}
	f, err := os.Create(to)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	w, _ := NewWriter(f)
	localAddress := net.ParseIP(trace.GetConnection().GetLocalAddress().GetSocketAddress().GetAddress())
	remoteAddress := net.ParseIP(trace.GetConnection().GetRemoteAddress().GetSocketAddress().GetAddress())
	for _, e := range trace.Events {
		t, err := ptypes.Timestamp(e.GetTimestamp())
		if err != nil {
			panic(err)
		}
		i := e.GetRead()
		if i != nil {
			data := i.GetData().GetAsBytes()
			err := w.WritePacket(t, &LayerInfo{
				SrcPort: layers.TCPPort(trace.GetConnection().GetRemoteAddress().GetSocketAddress().GetPortValue()),
				DstPort: layers.TCPPort(trace.GetConnection().GetLocalAddress().GetSocketAddress().GetPortValue()),
				SrcIP:   remoteAddress,
				DstIP:   localAddress,
			}, data, false)
			if err != nil {
				logutil.L().Info("read error", zap.Error(err))
			}
		}
		write := e.GetWrite()
		if write != nil {
			data := write.GetData().GetAsBytes()
			err := w.WritePacket(t, &LayerInfo{
				SrcPort: layers.TCPPort(trace.GetConnection().GetLocalAddress().GetSocketAddress().GetPortValue()),
				DstPort: layers.TCPPort(trace.GetConnection().GetRemoteAddress().GetSocketAddress().GetPortValue()),
				SrcIP:   localAddress,
				DstIP:   remoteAddress,
			}, data, true)
			if err != nil {
				logutil.L().Info("write error", zap.Error(err))
			}
		}
	}
}
