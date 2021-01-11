limelane
---

Envoy control plane for TCP proxy in Golang

Support TiKV as snapshot database backend.

### Features
- [x] TCP proxy EDS/CDS
- [x] Configurable HTTP API
- [x] Support persistent storage (TiKV)
- [x] Traffic tapping config for listener
- [ ] Metrics

### Tools
- gotap2pcap
  - Convert Envoy's tap trace Protocol Buffer file into a PCAP file, and it is only written in Golang.
- admintapclient
  - Envoy's admin /tap endpoint client (HTTP/2) written in Golang.

### Author
atpons

### License
MIT
