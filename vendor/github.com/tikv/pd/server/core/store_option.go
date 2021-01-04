// Copyright 2019 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/tikv/pd/server/core/storelimit"
)

// StoreCreateOption is used to create store.
type StoreCreateOption func(region *StoreInfo)

// SetStoreAddress sets the address for the store.
func SetStoreAddress(address, statusAddress, peerAddress string) StoreCreateOption {
	return func(store *StoreInfo) {
		meta := proto.Clone(store.meta).(*metapb.Store)
		meta.Address = address
		meta.StatusAddress = statusAddress
		meta.PeerAddress = peerAddress
		store.meta = meta
	}
}

// SetStoreLabels sets the labels for the store.
func SetStoreLabels(labels []*metapb.StoreLabel) StoreCreateOption {
	return func(store *StoreInfo) {
		meta := proto.Clone(store.meta).(*metapb.Store)
		meta.Labels = labels
		store.meta = meta
	}
}

// SetStoreStartTime sets the start timestamp for the store.
func SetStoreStartTime(startTS int64) StoreCreateOption {
	return func(store *StoreInfo) {
		meta := proto.Clone(store.meta).(*metapb.Store)
		meta.StartTimestamp = startTS
		store.meta = meta
	}
}

// SetStoreVersion sets the version for the store.
func SetStoreVersion(githash, version string) StoreCreateOption {
	return func(store *StoreInfo) {
		meta := proto.Clone(store.meta).(*metapb.Store)
		meta.Version = version
		meta.GitHash = githash
		store.meta = meta
	}
}

// SetStoreDeployPath sets the deploy path for the store.
func SetStoreDeployPath(deployPath string) StoreCreateOption {
	return func(store *StoreInfo) {
		meta := proto.Clone(store.meta).(*metapb.Store)
		meta.DeployPath = deployPath
		store.meta = meta
	}
}

// SetStoreState sets the state for the store.
func SetStoreState(state metapb.StoreState) StoreCreateOption {
	return func(store *StoreInfo) {
		meta := proto.Clone(store.meta).(*metapb.Store)
		meta.State = state
		store.meta = meta
	}
}

// PauseLeaderTransfer prevents the store from been selected as source or
// target store of TransferLeader.
func PauseLeaderTransfer() StoreCreateOption {
	return func(store *StoreInfo) {
		store.pauseLeaderTransfer = true
	}
}

// ResumeLeaderTransfer cleans a store's pause state. The store can be selected
// as source or target of TransferLeader again.
func ResumeLeaderTransfer() StoreCreateOption {
	return func(store *StoreInfo) {
		store.pauseLeaderTransfer = false
	}
}

// SetLeaderCount sets the leader count for the store.
func SetLeaderCount(leaderCount int) StoreCreateOption {
	return func(store *StoreInfo) {
		store.leaderCount = leaderCount
	}
}

// SetRegionCount sets the Region count for the store.
func SetRegionCount(regionCount int) StoreCreateOption {
	return func(store *StoreInfo) {
		store.regionCount = regionCount
	}
}

// SetPendingPeerCount sets the pending peer count for the store.
func SetPendingPeerCount(pendingPeerCount int) StoreCreateOption {
	return func(store *StoreInfo) {
		store.pendingPeerCount = pendingPeerCount
	}
}

// SetLeaderSize sets the leader size for the store.
func SetLeaderSize(leaderSize int64) StoreCreateOption {
	return func(store *StoreInfo) {
		store.leaderSize = leaderSize
	}
}

// SetRegionSize sets the Region size for the store.
func SetRegionSize(regionSize int64) StoreCreateOption {
	return func(store *StoreInfo) {
		store.regionSize = regionSize
	}
}

// SetLeaderWeight sets the leader weight for the store.
func SetLeaderWeight(leaderWeight float64) StoreCreateOption {
	return func(store *StoreInfo) {
		store.leaderWeight = leaderWeight
	}
}

// SetRegionWeight sets the Region weight for the store.
func SetRegionWeight(regionWeight float64) StoreCreateOption {
	return func(store *StoreInfo) {
		store.regionWeight = regionWeight
	}
}

// SetLastHeartbeatTS sets the time of last heartbeat for the store.
func SetLastHeartbeatTS(lastHeartbeatTS time.Time) StoreCreateOption {
	return func(store *StoreInfo) {
		store.meta.LastHeartbeat = lastHeartbeatTS.UnixNano()
	}
}

// SetLastPersistTime updates the time of last persistent.
func SetLastPersistTime(lastPersist time.Time) StoreCreateOption {
	return func(store *StoreInfo) {
		store.lastPersistTime = lastPersist
	}
}

// SetStoreStats sets the statistics information for the store.
func SetStoreStats(stats *pdpb.StoreStats) StoreCreateOption {
	return func(store *StoreInfo) {
		store.stats = stats
	}
}

// AttachAvailableFunc attaches a customize function for the store. The function f returns true if the store limit is not exceeded.
func AttachAvailableFunc(limitType storelimit.Type, f func() bool) StoreCreateOption {
	return func(store *StoreInfo) {
		if store.available == nil {
			store.available = make(map[storelimit.Type]func() bool)
		}
		store.available[limitType] = f
	}
}
