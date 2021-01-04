.PHONY: run envoy getenvoy

getenvoy:
	getenvoy run standard:1.16.2 -- --config-path `pwd`/envoy/envoy.yaml

run:
	CONFIG_FILE=`pwd`/envoy/config.yaml go run ./cmd/limelane/main.go
