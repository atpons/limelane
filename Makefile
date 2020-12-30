.PHONY: envoy getenvoy

envoy:
	docker run --name envoy --rm --network host -v `pwd`/envoy:/etc/envoy envoyproxy/envoy:v1.16.2

getenvoy:
	getenvoy run standard:1.16.2 -- --config-path `pwd`/envoy/envoy.yaml


run:
	CONFIG_FILE=`pwd`/envoy/config.yaml go run ./cmd/limeproxy/main.go
