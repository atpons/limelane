// Copyright 2016 TiKV Project Authors.
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
	"bytes"
	"encoding/hex"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"unsafe"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/replication_modepb"
)

// errRegionIsStale is error info for region is stale.
var errRegionIsStale = func(region *metapb.Region, origin *metapb.Region) error {
	return errors.Errorf("region is stale: region %v origin %v", region, origin)
}

// RegionInfo records detail region info.
// Read-Only once created.
type RegionInfo struct {
	term              uint64
	meta              *metapb.Region
	learners          []*metapb.Peer
	voters            []*metapb.Peer
	leader            *metapb.Peer
	downPeers         []*pdpb.PeerStats
	pendingPeers      []*metapb.Peer
	writtenBytes      uint64
	writtenKeys       uint64
	readBytes         uint64
	readKeys          uint64
	approximateSize   int64
	approximateKeys   int64
	interval          *pdpb.TimeInterval
	replicationStatus *replication_modepb.RegionReplicationStatus
}

// NewRegionInfo creates RegionInfo with region's meta and leader peer.
func NewRegionInfo(region *metapb.Region, leader *metapb.Peer, opts ...RegionCreateOption) *RegionInfo {
	regionInfo := &RegionInfo{
		meta:   region,
		leader: leader,
	}

	for _, opt := range opts {
		opt(regionInfo)
	}
	classifyVoterAndLearner(regionInfo)
	return regionInfo
}

// classifyVoterAndLearner sorts out voter and learner from peers into different slice.
func classifyVoterAndLearner(region *RegionInfo) {
	learners := make([]*metapb.Peer, 0, 1)
	voters := make([]*metapb.Peer, 0, len(region.meta.Peers))
	for _, p := range region.meta.Peers {
		if IsLearner(p) {
			learners = append(learners, p)
		} else {
			voters = append(voters, p)
		}
	}
	region.learners = learners
	region.voters = voters
}

// EmptyRegionApproximateSize is the region approximate size of an empty region
// (heartbeat size <= 1MB).
const EmptyRegionApproximateSize = 1

// RegionFromHeartbeat constructs a Region from region heartbeat.
func RegionFromHeartbeat(heartbeat *pdpb.RegionHeartbeatRequest) *RegionInfo {
	// Convert unit to MB.
	// If region is empty or less than 1MB, use 1MB instead.
	regionSize := heartbeat.GetApproximateSize() / (1 << 20)
	if regionSize < EmptyRegionApproximateSize {
		regionSize = EmptyRegionApproximateSize
	}

	region := &RegionInfo{
		term:              heartbeat.GetTerm(),
		meta:              heartbeat.GetRegion(),
		leader:            heartbeat.GetLeader(),
		downPeers:         heartbeat.GetDownPeers(),
		pendingPeers:      heartbeat.GetPendingPeers(),
		writtenBytes:      heartbeat.GetBytesWritten(),
		writtenKeys:       heartbeat.GetKeysWritten(),
		readBytes:         heartbeat.GetBytesRead(),
		readKeys:          heartbeat.GetKeysRead(),
		approximateSize:   int64(regionSize),
		approximateKeys:   int64(heartbeat.GetApproximateKeys()),
		interval:          heartbeat.GetInterval(),
		replicationStatus: heartbeat.GetReplicationStatus(),
	}

	classifyVoterAndLearner(region)
	return region
}

// Clone returns a copy of current regionInfo.
func (r *RegionInfo) Clone(opts ...RegionCreateOption) *RegionInfo {
	downPeers := make([]*pdpb.PeerStats, 0, len(r.downPeers))
	for _, peer := range r.downPeers {
		downPeers = append(downPeers, proto.Clone(peer).(*pdpb.PeerStats))
	}
	pendingPeers := make([]*metapb.Peer, 0, len(r.pendingPeers))
	for _, peer := range r.pendingPeers {
		pendingPeers = append(pendingPeers, proto.Clone(peer).(*metapb.Peer))
	}

	region := &RegionInfo{
		term:              r.term,
		meta:              proto.Clone(r.meta).(*metapb.Region),
		leader:            proto.Clone(r.leader).(*metapb.Peer),
		downPeers:         downPeers,
		pendingPeers:      pendingPeers,
		writtenBytes:      r.writtenBytes,
		writtenKeys:       r.writtenKeys,
		readBytes:         r.readBytes,
		readKeys:          r.readKeys,
		approximateSize:   r.approximateSize,
		approximateKeys:   r.approximateKeys,
		interval:          proto.Clone(r.interval).(*pdpb.TimeInterval),
		replicationStatus: r.replicationStatus,
	}

	for _, opt := range opts {
		opt(region)
	}
	classifyVoterAndLearner(region)
	return region
}

// GetTerm returns the current term of the region
func (r *RegionInfo) GetTerm() uint64 {
	return r.term
}

// GetLearners returns the learners.
func (r *RegionInfo) GetLearners() []*metapb.Peer {
	return r.learners
}

// GetVoters returns the voters.
func (r *RegionInfo) GetVoters() []*metapb.Peer {
	return r.voters
}

// GetPeer returns the peer with specified peer id.
func (r *RegionInfo) GetPeer(peerID uint64) *metapb.Peer {
	for _, peer := range r.meta.GetPeers() {
		if peer.GetId() == peerID {
			return peer
		}
	}
	return nil
}

// GetDownPeer returns the down peer with specified peer id.
func (r *RegionInfo) GetDownPeer(peerID uint64) *metapb.Peer {
	for _, down := range r.downPeers {
		if down.GetPeer().GetId() == peerID {
			return down.GetPeer()
		}
	}
	return nil
}

// GetDownVoter returns the down voter with specified peer id.
func (r *RegionInfo) GetDownVoter(peerID uint64) *metapb.Peer {
	for _, down := range r.downPeers {
		if down.GetPeer().GetId() == peerID && !IsLearner(down.GetPeer()) {
			return down.GetPeer()
		}
	}
	return nil
}

// GetDownLearner returns the down learner with soecified peer id.
func (r *RegionInfo) GetDownLearner(peerID uint64) *metapb.Peer {
	for _, down := range r.downPeers {
		if down.GetPeer().GetId() == peerID && IsLearner(down.GetPeer()) {
			return down.GetPeer()
		}
	}
	return nil
}

// GetPendingPeer returns the pending peer with specified peer id.
func (r *RegionInfo) GetPendingPeer(peerID uint64) *metapb.Peer {
	for _, peer := range r.pendingPeers {
		if peer.GetId() == peerID {
			return peer
		}
	}
	return nil
}

// GetPendingVoter returns the pending voter with specified peer id.
func (r *RegionInfo) GetPendingVoter(peerID uint64) *metapb.Peer {
	for _, peer := range r.pendingPeers {
		if peer.GetId() == peerID && !IsLearner(peer) {
			return peer
		}
	}
	return nil
}

// GetPendingLearner returns the pending learner peer with specified peer id.
func (r *RegionInfo) GetPendingLearner(peerID uint64) *metapb.Peer {
	for _, peer := range r.pendingPeers {
		if peer.GetId() == peerID && IsLearner(peer) {
			return peer
		}
	}
	return nil
}

// GetStorePeer returns the peer in specified store.
func (r *RegionInfo) GetStorePeer(storeID uint64) *metapb.Peer {
	for _, peer := range r.meta.GetPeers() {
		if peer.GetStoreId() == storeID {
			return peer
		}
	}
	return nil
}

// GetStoreVoter returns the voter in specified store.
func (r *RegionInfo) GetStoreVoter(storeID uint64) *metapb.Peer {
	for _, peer := range r.voters {
		if peer.GetStoreId() == storeID {
			return peer
		}
	}
	return nil
}

// GetStoreLearner returns the learner peer in specified store.
func (r *RegionInfo) GetStoreLearner(storeID uint64) *metapb.Peer {
	for _, peer := range r.learners {
		if peer.GetStoreId() == storeID {
			return peer
		}
	}
	return nil
}

// GetStoreIds returns a map indicate the region distributed.
func (r *RegionInfo) GetStoreIds() map[uint64]struct{} {
	peers := r.meta.GetPeers()
	stores := make(map[uint64]struct{}, len(peers))
	for _, peer := range peers {
		stores[peer.GetStoreId()] = struct{}{}
	}
	return stores
}

// GetFollowers returns a map indicate the follow peers distributed.
func (r *RegionInfo) GetFollowers() map[uint64]*metapb.Peer {
	peers := r.GetVoters()
	followers := make(map[uint64]*metapb.Peer, len(peers))
	for _, peer := range peers {
		if r.leader == nil || r.leader.GetId() != peer.GetId() {
			followers[peer.GetStoreId()] = peer
		}
	}
	return followers
}

// GetFollower randomly returns a follow peer.
func (r *RegionInfo) GetFollower() *metapb.Peer {
	for _, peer := range r.GetVoters() {
		if r.leader == nil || r.leader.GetId() != peer.GetId() {
			return peer
		}
	}
	return nil
}

// GetDiffFollowers returns the followers which is not located in the same
// store as any other followers of the another specified region.
func (r *RegionInfo) GetDiffFollowers(other *RegionInfo) []*metapb.Peer {
	res := make([]*metapb.Peer, 0, len(r.meta.Peers))
	for _, p := range r.GetFollowers() {
		diff := true
		for _, o := range other.GetFollowers() {
			if p.GetStoreId() == o.GetStoreId() {
				diff = false
				break
			}
		}
		if diff {
			res = append(res, p)
		}
	}
	return res
}

// GetID returns the ID of the region.
func (r *RegionInfo) GetID() uint64 {
	return r.meta.GetId()
}

// GetMeta returns the meta information of the region.
func (r *RegionInfo) GetMeta() *metapb.Region {
	if r == nil {
		return nil
	}
	return r.meta
}

// GetStat returns the statistics of the region.
func (r *RegionInfo) GetStat() *pdpb.RegionStat {
	if r == nil {
		return nil
	}
	return &pdpb.RegionStat{
		BytesWritten: r.writtenBytes,
		BytesRead:    r.readBytes,
		KeysWritten:  r.writtenKeys,
		KeysRead:     r.readKeys,
	}
}

// GetApproximateSize returns the approximate size of the region.
func (r *RegionInfo) GetApproximateSize() int64 {
	return r.approximateSize
}

// GetApproximateKeys returns the approximate keys of the region.
func (r *RegionInfo) GetApproximateKeys() int64 {
	return r.approximateKeys
}

// GetInterval returns the interval information of the region.
func (r *RegionInfo) GetInterval() *pdpb.TimeInterval {
	return r.interval
}

// GetDownPeers returns the down peers of the region.
func (r *RegionInfo) GetDownPeers() []*pdpb.PeerStats {
	return r.downPeers
}

// GetPendingPeers returns the pending peers of the region.
func (r *RegionInfo) GetPendingPeers() []*metapb.Peer {
	return r.pendingPeers
}

// GetBytesRead returns the read bytes of the region.
func (r *RegionInfo) GetBytesRead() uint64 {
	return r.readBytes
}

// GetBytesWritten returns the written bytes of the region.
func (r *RegionInfo) GetBytesWritten() uint64 {
	return r.writtenBytes
}

// GetKeysWritten returns the written keys of the region.
func (r *RegionInfo) GetKeysWritten() uint64 {
	return r.writtenKeys
}

// GetKeysRead returns the read keys of the region.
func (r *RegionInfo) GetKeysRead() uint64 {
	return r.readKeys
}

// GetLeader returns the leader of the region.
func (r *RegionInfo) GetLeader() *metapb.Peer {
	return r.leader
}

// GetStartKey returns the start key of the region.
func (r *RegionInfo) GetStartKey() []byte {
	return r.meta.StartKey
}

// GetEndKey returns the end key of the region.
func (r *RegionInfo) GetEndKey() []byte {
	return r.meta.EndKey
}

// GetPeers returns the peers of the region.
func (r *RegionInfo) GetPeers() []*metapb.Peer {
	return r.meta.GetPeers()
}

// GetRegionEpoch returns the region epoch of the region.
func (r *RegionInfo) GetRegionEpoch() *metapb.RegionEpoch {
	return r.meta.RegionEpoch
}

// GetReplicationStatus returns the region's replication status.
func (r *RegionInfo) GetReplicationStatus() *replication_modepb.RegionReplicationStatus {
	return r.replicationStatus
}

// regionMap wraps a map[uint64]*core.RegionInfo and supports randomly pick a region.
type regionMap struct {
	m         map[uint64]*RegionInfo
	totalSize int64
	totalKeys int64
}

func newRegionMap() *regionMap {
	return &regionMap{
		m: make(map[uint64]*RegionInfo),
	}
}

func (rm *regionMap) Len() int {
	if rm == nil {
		return 0
	}
	return len(rm.m)
}

func (rm *regionMap) Get(id uint64) *RegionInfo {
	if rm == nil {
		return nil
	}
	if r, ok := rm.m[id]; ok {
		return r
	}
	return nil
}

func (rm *regionMap) Put(region *RegionInfo) {
	if old, ok := rm.m[region.GetID()]; ok {
		rm.totalSize -= old.approximateSize
		rm.totalKeys -= old.approximateKeys
	}
	rm.m[region.GetID()] = region
	rm.totalSize += region.approximateSize
	rm.totalKeys += region.approximateKeys
}

func (rm *regionMap) Delete(id uint64) {
	if rm == nil {
		return
	}
	if old, ok := rm.m[id]; ok {
		delete(rm.m, id)
		rm.totalSize -= old.approximateSize
		rm.totalKeys -= old.approximateKeys
	}
}

func (rm *regionMap) TotalSize() int64 {
	if rm.Len() == 0 {
		return 0
	}
	return rm.totalSize
}

// regionSubTree is used to manager different types of regions.
type regionSubTree struct {
	*regionTree
	totalSize int64
	totalKeys int64
}

func newRegionSubTree() *regionSubTree {
	return &regionSubTree{
		regionTree: newRegionTree(),
		totalSize:  0,
	}
}

func (rst *regionSubTree) TotalSize() int64 {
	if rst.length() == 0 {
		return 0
	}
	return rst.totalSize
}

func (rst *regionSubTree) scanRanges() []*RegionInfo {
	if rst.length() == 0 {
		return nil
	}
	var res []*RegionInfo
	rst.scanRange([]byte(""), func(region *RegionInfo) bool {
		res = append(res, region)
		return true
	})
	return res
}

func (rst *regionSubTree) update(region *RegionInfo) {
	overlaps := rst.regionTree.update(region)
	rst.totalSize += region.approximateSize
	rst.totalKeys += region.approximateKeys
	for _, r := range overlaps {
		rst.totalSize -= r.approximateSize
		rst.totalKeys -= r.approximateKeys
	}
}

func (rst *regionSubTree) remove(region *RegionInfo) {
	if rst.length() == 0 {
		return
	}
	if rst.regionTree.remove(region) != nil {
		rst.totalSize -= region.approximateSize
		rst.totalKeys -= region.approximateKeys
	}
}

func (rst *regionSubTree) length() int {
	if rst == nil {
		return 0
	}
	return rst.regionTree.length()
}

func (rst *regionSubTree) RandomRegion(ranges []KeyRange) *RegionInfo {
	if rst.length() == 0 {
		return nil
	}

	return rst.regionTree.RandomRegion(ranges)
}

func (rst *regionSubTree) RandomRegions(n int, ranges []KeyRange) []*RegionInfo {
	if rst.length() == 0 {
		return nil
	}

	regions := make([]*RegionInfo, 0, n)

	for i := 0; i < n; i++ {
		if region := rst.regionTree.RandomRegion(ranges); region != nil {
			regions = append(regions, region)
		}
	}
	return regions
}

// RegionsInfo for export
type RegionsInfo struct {
	tree         *regionTree
	regions      *regionMap                // regionID -> regionInfo
	leaders      map[uint64]*regionSubTree // storeID -> regionSubTree
	followers    map[uint64]*regionSubTree // storeID -> regionSubTree
	learners     map[uint64]*regionSubTree // storeID -> regionSubTree
	pendingPeers map[uint64]*regionSubTree // storeID -> regionSubTree
}

// NewRegionsInfo creates RegionsInfo with tree, regions, leaders and followers
func NewRegionsInfo() *RegionsInfo {
	return &RegionsInfo{
		tree:         newRegionTree(),
		regions:      newRegionMap(),
		leaders:      make(map[uint64]*regionSubTree),
		followers:    make(map[uint64]*regionSubTree),
		learners:     make(map[uint64]*regionSubTree),
		pendingPeers: make(map[uint64]*regionSubTree),
	}
}

// GetRegion returns the RegionInfo with regionID
func (r *RegionsInfo) GetRegion(regionID uint64) *RegionInfo {
	region := r.regions.Get(regionID)
	if region == nil {
		return nil
	}
	return region
}

// SetRegion sets the RegionInfo with regionID
func (r *RegionsInfo) SetRegion(region *RegionInfo) []*RegionInfo {
	if origin := r.regions.Get(region.GetID()); origin != nil {
		if !bytes.Equal(origin.GetStartKey(), region.GetStartKey()) || !bytes.Equal(origin.GetEndKey(), region.GetEndKey()) {
			r.removeRegionFromTreeAndMap(origin)
		}
		if r.shouldRemoveFromSubTree(region, origin) {
			r.removeRegionFromSubTree(origin)
		}
	}
	return r.AddRegion(region)
}

// Length returns the RegionsInfo length
func (r *RegionsInfo) Length() int {
	return r.regions.Len()
}

// TreeLength returns the RegionsInfo tree length(now only used in test)
func (r *RegionsInfo) TreeLength() int {
	return r.tree.length()
}

// GetOverlaps returns the regions which are overlapped with the specified region range.
func (r *RegionsInfo) GetOverlaps(region *RegionInfo) []*RegionInfo {
	return r.tree.getOverlaps(region)
}

// AddRegion adds RegionInfo to regionTree and regionMap, also update leaders and followers by region peers
func (r *RegionsInfo) AddRegion(region *RegionInfo) []*RegionInfo {
	// the regions which are overlapped with the specified region range.
	var overlaps []*RegionInfo
	// when the value is true, add the region to the tree. otherwise use the region replace the origin region in the tree.
	treeNeedAdd := true
	if origin := r.GetRegion(region.GetID()); origin != nil {
		if regionOld := r.tree.find(region); regionOld != nil {
			// Update to tree.
			if bytes.Equal(regionOld.region.GetStartKey(), region.GetStartKey()) &&
				bytes.Equal(regionOld.region.GetEndKey(), region.GetEndKey()) &&
				regionOld.region.GetID() == region.GetID() {
				regionOld.region = region
				treeNeedAdd = false
			}
		}
	}
	if treeNeedAdd {
		// Add to tree.
		overlaps = r.tree.update(region)
		for _, item := range overlaps {
			r.RemoveRegion(r.GetRegion(item.GetID()))
		}
	}
	// Add to regions.
	r.regions.Put(region)

	// Add to leaders and followers.
	for _, peer := range region.GetVoters() {
		storeID := peer.GetStoreId()
		if peer.GetId() == region.leader.GetId() {
			// Add leader peer to leaders.
			store, ok := r.leaders[storeID]
			if !ok {
				store = newRegionSubTree()
				r.leaders[storeID] = store
			}
			store.update(region)
		} else {
			// Add follower peer to followers.
			store, ok := r.followers[storeID]
			if !ok {
				store = newRegionSubTree()
				r.followers[storeID] = store
			}
			store.update(region)
		}
	}

	// Add to learners.
	for _, peer := range region.GetLearners() {
		storeID := peer.GetStoreId()
		store, ok := r.learners[storeID]
		if !ok {
			store = newRegionSubTree()
			r.learners[storeID] = store
		}
		store.update(region)
	}

	for _, peer := range region.pendingPeers {
		storeID := peer.GetStoreId()
		store, ok := r.pendingPeers[storeID]
		if !ok {
			store = newRegionSubTree()
			r.pendingPeers[storeID] = store
		}
		store.update(region)
	}

	return overlaps
}

// RemoveRegion removes RegionInfo from regionTree and regionMap
func (r *RegionsInfo) RemoveRegion(region *RegionInfo) {
	// Remove from tree and regions.
	r.removeRegionFromTreeAndMap(region)
	// Remove from leaders and followers.
	r.removeRegionFromSubTree(region)
}

// removeRegionFromTreeAndMap removes RegionInfo from regionTree and regionMap
func (r *RegionsInfo) removeRegionFromTreeAndMap(region *RegionInfo) {
	// Remove from tree and regions.
	r.tree.remove(region)
	r.regions.Delete(region.GetID())
}

// removeRegionFromSubTree removes RegionInfo from regionSubTrees
func (r *RegionsInfo) removeRegionFromSubTree(region *RegionInfo) {
	// Remove from leaders and followers.
	for _, peer := range region.meta.GetPeers() {
		storeID := peer.GetStoreId()
		r.leaders[storeID].remove(region)
		r.followers[storeID].remove(region)
		r.learners[storeID].remove(region)
		r.pendingPeers[storeID].remove(region)
	}
}

type peerSlice []*metapb.Peer

func (s peerSlice) Len() int {
	return len(s)
}
func (s peerSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s peerSlice) Less(i, j int) bool {
	return s[i].GetId() < s[j].GetId()
}

// shouldRemoveFromSubTree return true when the region leader changed, peer transferred,
// new peer was created, learners changed, pendingPeers changed, and so on.
func (r *RegionsInfo) shouldRemoveFromSubTree(region *RegionInfo, origin *RegionInfo) bool {
	checkPeersChange := func(origin []*metapb.Peer, other []*metapb.Peer) bool {
		if len(origin) != len(other) {
			return true
		}
		sort.Sort(peerSlice(origin))
		sort.Sort(peerSlice(other))
		for index, peer := range origin {
			if peer.GetStoreId() == other[index].GetStoreId() && peer.GetId() == other[index].GetId() {
				continue
			}
			return true
		}
		return false
	}

	return origin.leader.GetId() != region.leader.GetId() ||
		checkPeersChange(origin.GetVoters(), region.GetVoters()) ||
		checkPeersChange(origin.GetLearners(), region.GetLearners()) ||
		checkPeersChange(origin.GetPendingPeers(), region.GetPendingPeers())
}

// SearchRegion searches RegionInfo from regionTree
func (r *RegionsInfo) SearchRegion(regionKey []byte) *RegionInfo {
	region := r.tree.search(regionKey)
	if region == nil {
		return nil
	}
	return r.GetRegion(region.GetID())
}

// SearchPrevRegion searches previous RegionInfo from regionTree
func (r *RegionsInfo) SearchPrevRegion(regionKey []byte) *RegionInfo {
	region := r.tree.searchPrev(regionKey)
	if region == nil {
		return nil
	}
	return r.GetRegion(region.GetID())
}

// GetRegions gets all RegionInfo from regionMap
func (r *RegionsInfo) GetRegions() []*RegionInfo {
	regions := make([]*RegionInfo, 0, r.regions.Len())
	for _, region := range r.regions.m {
		regions = append(regions, region)
	}
	return regions
}

// GetStoreRegions gets all RegionInfo with a given storeID
func (r *RegionsInfo) GetStoreRegions(storeID uint64) []*RegionInfo {
	regions := make([]*RegionInfo, 0, r.GetStoreRegionCount(storeID))
	if leaders, ok := r.leaders[storeID]; ok {
		regions = append(regions, leaders.scanRanges()...)
	}
	if followers, ok := r.followers[storeID]; ok {
		regions = append(regions, followers.scanRanges()...)
	}
	if learners, ok := r.learners[storeID]; ok {
		regions = append(regions, learners.scanRanges()...)
	}
	return regions
}

// GetStoreLeaderRegionSize get total size of store's leader regions
func (r *RegionsInfo) GetStoreLeaderRegionSize(storeID uint64) int64 {
	return r.leaders[storeID].TotalSize()
}

// GetStoreFollowerRegionSize get total size of store's follower regions
func (r *RegionsInfo) GetStoreFollowerRegionSize(storeID uint64) int64 {
	return r.followers[storeID].TotalSize()
}

// GetStoreLearnerRegionSize get total size of store's learner regions
func (r *RegionsInfo) GetStoreLearnerRegionSize(storeID uint64) int64 {
	return r.learners[storeID].TotalSize()
}

// GetStoreRegionSize get total size of store's regions
func (r *RegionsInfo) GetStoreRegionSize(storeID uint64) int64 {
	return r.GetStoreLeaderRegionSize(storeID) + r.GetStoreFollowerRegionSize(storeID) + r.GetStoreLearnerRegionSize(storeID)
}

// GetMetaRegions gets a set of metapb.Region from regionMap
func (r *RegionsInfo) GetMetaRegions() []*metapb.Region {
	regions := make([]*metapb.Region, 0, r.regions.Len())
	for _, region := range r.regions.m {
		regions = append(regions, proto.Clone(region.meta).(*metapb.Region))
	}
	return regions
}

// GetRegionCount gets the total count of RegionInfo of regionMap
func (r *RegionsInfo) GetRegionCount() int {
	return r.regions.Len()
}

// GetStoreRegionCount gets the total count of a store's leader, follower and learner RegionInfo by storeID
func (r *RegionsInfo) GetStoreRegionCount(storeID uint64) int {
	return r.GetStoreLeaderCount(storeID) + r.GetStoreFollowerCount(storeID) + r.GetStoreLearnerCount(storeID)
}

// GetStorePendingPeerCount gets the total count of a store's region that includes pending peer
func (r *RegionsInfo) GetStorePendingPeerCount(storeID uint64) int {
	return r.pendingPeers[storeID].length()
}

// GetStoreLeaderCount get the total count of a store's leader RegionInfo
func (r *RegionsInfo) GetStoreLeaderCount(storeID uint64) int {
	return r.leaders[storeID].length()
}

// GetStoreFollowerCount get the total count of a store's follower RegionInfo
func (r *RegionsInfo) GetStoreFollowerCount(storeID uint64) int {
	return r.followers[storeID].length()
}

// GetStoreLearnerCount get the total count of a store's learner RegionInfo
func (r *RegionsInfo) GetStoreLearnerCount(storeID uint64) int {
	return r.learners[storeID].length()
}

// RandPendingRegion randomly gets a store's region with a pending peer.
func (r *RegionsInfo) RandPendingRegion(storeID uint64, ranges []KeyRange) *RegionInfo {
	return r.pendingPeers[storeID].RandomRegion(ranges)
}

// RandPendingRegions randomly gets a store's n regions with a pending peer.
func (r *RegionsInfo) RandPendingRegions(storeID uint64, ranges []KeyRange, n int) []*RegionInfo {
	return r.pendingPeers[storeID].RandomRegions(n, ranges)
}

// RandLeaderRegion randomly gets a store's leader region.
func (r *RegionsInfo) RandLeaderRegion(storeID uint64, ranges []KeyRange) *RegionInfo {
	return r.leaders[storeID].RandomRegion(ranges)
}

// RandLeaderRegions randomly gets a store's n leader regions.
func (r *RegionsInfo) RandLeaderRegions(storeID uint64, ranges []KeyRange, n int) []*RegionInfo {
	return r.leaders[storeID].RandomRegions(n, ranges)
}

// RandFollowerRegion randomly gets a store's follower region.
func (r *RegionsInfo) RandFollowerRegion(storeID uint64, ranges []KeyRange) *RegionInfo {
	return r.followers[storeID].RandomRegion(ranges)
}

// RandFollowerRegions randomly gets a store's n follower regions.
func (r *RegionsInfo) RandFollowerRegions(storeID uint64, ranges []KeyRange, n int) []*RegionInfo {
	return r.followers[storeID].RandomRegions(n, ranges)
}

// RandLearnerRegion randomly gets a store's learner region.
func (r *RegionsInfo) RandLearnerRegion(storeID uint64, ranges []KeyRange) *RegionInfo {
	return r.learners[storeID].RandomRegion(ranges)
}

// RandLearnerRegions randomly gets a store's n learner regions.
func (r *RegionsInfo) RandLearnerRegions(storeID uint64, ranges []KeyRange, n int) []*RegionInfo {
	return r.learners[storeID].RandomRegions(n, ranges)
}

// GetLeader return leader RegionInfo by storeID and regionID(now only used in test)
func (r *RegionsInfo) GetLeader(storeID uint64, region *RegionInfo) *RegionInfo {
	if leaders, ok := r.leaders[storeID]; ok {
		return leaders.find(region).region
	}
	return nil
}

// GetFollower return follower RegionInfo by storeID and regionID(now only used in test)
func (r *RegionsInfo) GetFollower(storeID uint64, region *RegionInfo) *RegionInfo {
	if followers, ok := r.followers[storeID]; ok {
		return followers.find(region).region
	}
	return nil
}

// ScanRange scans regions intersecting [start key, end key), returns at most
// `limit` regions. limit <= 0 means no limit.
func (r *RegionsInfo) ScanRange(startKey, endKey []byte, limit int) []*RegionInfo {
	var res []*RegionInfo
	r.tree.scanRange(startKey, func(region *RegionInfo) bool {
		if len(endKey) > 0 && bytes.Compare(region.GetStartKey(), endKey) >= 0 {
			return false
		}
		if limit > 0 && len(res) >= limit {
			return false
		}
		res = append(res, r.GetRegion(region.GetID()))
		return true
	})
	return res
}

// ScanRangeWithIterator scans from the first region containing or behind start key,
// until iterator returns false.
func (r *RegionsInfo) ScanRangeWithIterator(startKey []byte, iterator func(region *RegionInfo) bool) {
	r.tree.scanRange(startKey, iterator)
}

// GetAdjacentRegions returns region's info that is adjacent with specific region
func (r *RegionsInfo) GetAdjacentRegions(region *RegionInfo) (*RegionInfo, *RegionInfo) {
	p, n := r.tree.getAdjacentRegions(region)
	var prev, next *RegionInfo
	// check key to avoid key range hole
	if p != nil && bytes.Equal(p.region.GetEndKey(), region.GetStartKey()) {
		prev = r.GetRegion(p.region.GetID())
	}
	if n != nil && bytes.Equal(region.GetEndKey(), n.region.GetStartKey()) {
		next = r.GetRegion(n.region.GetID())
	}
	return prev, next
}

// GetAverageRegionSize returns the average region approximate size.
func (r *RegionsInfo) GetAverageRegionSize() int64 {
	if r.regions.Len() == 0 {
		return 0
	}
	return r.regions.TotalSize() / int64(r.regions.Len())
}

// DiffRegionPeersInfo return the difference of peers info  between two RegionInfo
func DiffRegionPeersInfo(origin *RegionInfo, other *RegionInfo) string {
	var ret []string
	for _, a := range origin.meta.Peers {
		both := false
		for _, b := range other.meta.Peers {
			if reflect.DeepEqual(a, b) {
				both = true
				break
			}
		}
		if !both {
			ret = append(ret, fmt.Sprintf("Remove peer:{%v}", a))
		}
	}
	for _, b := range other.meta.Peers {
		both := false
		for _, a := range origin.meta.Peers {
			if reflect.DeepEqual(a, b) {
				both = true
				break
			}
		}
		if !both {
			ret = append(ret, fmt.Sprintf("Add peer:{%v}", b))
		}
	}
	return strings.Join(ret, ",")
}

// DiffRegionKeyInfo return the difference of key info between two RegionInfo
func DiffRegionKeyInfo(origin *RegionInfo, other *RegionInfo) string {
	var ret []string
	if !bytes.Equal(origin.meta.StartKey, other.meta.StartKey) {
		ret = append(ret, fmt.Sprintf("StartKey Changed:{%s} -> {%s}", HexRegionKey(origin.meta.StartKey), HexRegionKey(other.meta.StartKey)))
	} else {
		ret = append(ret, fmt.Sprintf("StartKey:{%s}", HexRegionKey(origin.meta.StartKey)))
	}
	if !bytes.Equal(origin.meta.EndKey, other.meta.EndKey) {
		ret = append(ret, fmt.Sprintf("EndKey Changed:{%s} -> {%s}", HexRegionKey(origin.meta.EndKey), HexRegionKey(other.meta.EndKey)))
	} else {
		ret = append(ret, fmt.Sprintf("EndKey:{%s}", HexRegionKey(origin.meta.EndKey)))
	}

	return strings.Join(ret, ", ")
}

func isInvolved(region *RegionInfo, startKey, endKey []byte) bool {
	return bytes.Compare(region.GetStartKey(), startKey) >= 0 && (len(endKey) == 0 || (len(region.GetEndKey()) > 0 && bytes.Compare(region.GetEndKey(), endKey) <= 0))
}

// String converts slice of bytes to string without copy.
func String(b []byte) (s string) {
	if len(b) == 0 {
		return ""
	}
	pbytes := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	pstring := (*reflect.StringHeader)(unsafe.Pointer(&s))
	pstring.Data = pbytes.Data
	pstring.Len = pbytes.Len
	return
}

// ToUpperASCIIInplace bytes.ToUpper but zero-cost
func ToUpperASCIIInplace(s []byte) []byte {
	hasLower := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		hasLower = hasLower || ('a' <= c && c <= 'z')
	}

	if !hasLower {
		return s
	}
	var c byte
	for i := 0; i < len(s); i++ {
		c = s[i]
		if 'a' <= c && c <= 'z' {
			c -= 'a' - 'A'
		}
		s[i] = c
	}
	return s
}

// EncodeToString overrides hex.EncodeToString implementation. Difference: returns []byte, not string
func EncodeToString(src []byte) []byte {
	dst := make([]byte, hex.EncodedLen(len(src)))
	hex.Encode(dst, src)
	return dst
}

// HexRegionKey converts region key to hex format. Used for formating region in
// logs.
func HexRegionKey(key []byte) []byte {
	return ToUpperASCIIInplace(EncodeToString(key))
}

// HexRegionKeyStr converts region key to hex format. Used for formating region in
// logs.
func HexRegionKeyStr(key []byte) string {
	return String(HexRegionKey(key))
}

// RegionToHexMeta converts a region meta's keys to hex format. Used for formating
// region in logs.
func RegionToHexMeta(meta *metapb.Region) HexRegionMeta {
	if meta == nil {
		return HexRegionMeta{}
	}
	meta = proto.Clone(meta).(*metapb.Region)
	meta.StartKey = HexRegionKey(meta.StartKey)
	meta.EndKey = HexRegionKey(meta.EndKey)
	return HexRegionMeta{meta}
}

// HexRegionMeta is a region meta in the hex format. Used for formating region in logs.
type HexRegionMeta struct {
	*metapb.Region
}

func (h HexRegionMeta) String() string {
	return strings.TrimSpace(proto.CompactTextString(h.Region))
}

// RegionsToHexMeta converts regions' meta keys to hex format. Used for formating
// region in logs.
func RegionsToHexMeta(regions []*metapb.Region) HexRegionsMeta {
	hexRegionMetas := make([]*metapb.Region, len(regions))
	for i, region := range regions {
		meta := proto.Clone(region).(*metapb.Region)
		meta.StartKey = HexRegionKey(meta.StartKey)
		meta.EndKey = HexRegionKey(meta.EndKey)

		hexRegionMetas[i] = meta
	}
	return hexRegionMetas
}

// HexRegionsMeta is a slice of regions' meta in the hex format. Used for formating
// region in logs.
type HexRegionsMeta []*metapb.Region

func (h HexRegionsMeta) String() string {
	var b strings.Builder
	for _, r := range h {
		b.WriteString(proto.CompactTextString(r))
	}
	return strings.TrimSpace(b.String())
}
