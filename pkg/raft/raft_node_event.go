package raft

import (
	"github.com/lni/dragonboat/v4/raftio"
)

func (p *nodeHostEvent) LeaderUpdated(info raftio.LeaderInfo) {
	p.event.LeaderUpdated(info.LeaderID, info.ShardID, info.ReplicaID, info.Term)
}

func (p *nodeHostEvent) NodeHostShuttingDown() {
	p.event.NodeShuttingDown()
}

func (p *nodeHostEvent) NodeUnloaded(info raftio.NodeInfo) {
	p.event.NodeUnloaded(info.ReplicaID, info.ShardID)
}

func (p *nodeHostEvent) NodeDeleted(info raftio.NodeInfo) {
	p.event.NodeDeleted(info.ReplicaID, info.ShardID)
}

func (p *nodeHostEvent) NodeReady(info raftio.NodeInfo) {
	p.event.NodeReady(info.ReplicaID, info.ShardID)
}

func (p *nodeHostEvent) MembershipChanged(info raftio.NodeInfo) {
	p.event.MembershipChanged(info.ReplicaID, info.ShardID)
}

func (p *nodeHostEvent) ConnectionEstablished(info raftio.ConnectionInfo) {
	p.event.ConnectionEstablished(info.Address, info.SnapshotConnection)
}

func (p *nodeHostEvent) ConnectionFailed(info raftio.ConnectionInfo) {
	p.event.ConnectionFailed(info.Address, info.SnapshotConnection)
}

func (p *nodeHostEvent) SendSnapshotStarted(_ raftio.SnapshotInfo)   {}
func (p *nodeHostEvent) SendSnapshotCompleted(_ raftio.SnapshotInfo) {}
func (p *nodeHostEvent) SendSnapshotAborted(_ raftio.SnapshotInfo)   {}
func (p *nodeHostEvent) SnapshotReceived(_ raftio.SnapshotInfo)      {}
func (p *nodeHostEvent) SnapshotRecovered(_ raftio.SnapshotInfo)     {}
func (p *nodeHostEvent) SnapshotCreated(_ raftio.SnapshotInfo)       {}
func (p *nodeHostEvent) SnapshotCompacted(_ raftio.SnapshotInfo)     {}
func (p *nodeHostEvent) LogCompacted(_ raftio.EntryInfo)             {}
func (p *nodeHostEvent) LogDBCompacted(_ raftio.EntryInfo)           {}
