package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
)

const (
	InnerEventMessage  = "_inner_"
	InnerEventResponse = "_resp_"
)

// EventHandler 消息回调数据
// replicaID - 请求者ID
// eventName - 事件名称
// data - 事件消息内容
// response - 需要返回什么数据
type EventHandler func(replicaID uint64, eventName string, data string) (response string)

// EventListener Raft事件监听
type EventListener interface {
	// LogUpdated 触发日志已更新
	LogUpdated(log []byte, index uint64)
	// LogRead 触发日志读取
	LogRead(i interface{})
	// LeaderUpdated Leader更新
	LeaderUpdated(leaderID, shardID, replicaID, term uint64)

	// NodeShuttingDown 节点关闭中
	NodeShuttingDown()
	// NodeUnloaded 节点被卸载
	NodeUnloaded(replicaID, shardID uint64)
	// NodeDeleted 节点被删除
	NodeDeleted(replicaID, shardID uint64)
	// NodeReady 节点可正常服务
	NodeReady(replicaID, shardID uint64)
	// MembershipChanged 集群内节点成员发生变化
	MembershipChanged(replicaID, shardID uint64)

	// ConnectionEstablished 已建立Raft连接
	ConnectionEstablished(address string, snapshot bool)
	// ConnectionFailed 建立Raft连接失败
	ConnectionFailed(address string, snapshot bool)
}

type nodeHostEvent struct{ *RaftService }

// RaftService Raft服务结构
type RaftService struct {
	// replicaID 节点ID, 在同一个集群内要保证唯一
	replicaID uint64

	// shardID 集群ID, 可以存在多个集群
	// 例如三个节点ID:1,2,3,如果想搭建在同一个集群则同时设置集群ID为100
	// 1,100
	// 2,100
	// 3,100
	shardID uint64

	// closed 是否已关闭
	closed atomic.Bool

	// join 是否半截新增的节点
	// 如果是新增的节点members必须为空(通过加入的ShardID内获取)
	join bool

	// 初始化集群ID+IP地址列表
	// 例如三个节点ID:1,2,3,如果想搭建在同一个集群则同时设置集群ID为100
	// 1,100
	// 2,100
	// 3,100
	// 该结构为[1:"192.168.0.1", 2:"192.168.0.2", 3:"192.168.0.3"]
	members map[uint64]string

	// raftConfig Raft配置
	// v4版dragon boat支持preVote
	// 详见Config内注释
	raftConfig config.Config

	// hostConfig 节点配置,详见Config内注释
	hostConfig config.NodeHostConfig

	// raft Raft实例对象
	raft *dragonboat.NodeHost

	// kv 用于持久化数据
	kv sync.Map

	// event 事件监听器
	event EventListener

	// stateMachine 状态机
	stateMachine *stateMachine

	// nodeHostEvent Raft内部事件
	nhEvent *nodeHostEvent

	// registerEvents 注册的事件列表
	registerEvents sync.Map

	// responseMessage 回复的消息列表
	responseMessage sync.Map
}

// New 新建一个Raft服务
// replicaID 					- 节点ID(1)
// shardID 						- 集群ID(100)
// electionRTT与heartbeatRTT 	- 选举的RTT(要大于heartbeatRTT, 接近整体节点的平均值, 假设3个节点electionRTT = 100, heartbeatRTT 33)
// rttMillisecond 				- 集群内通信的延迟(毫秒)网络越好数字推荐越小(200)
// snapshotEntries 				- 快照自动备份的频率, 默认关闭即可(0)
// compactionOverhead 			- 切割日志时最后保留多少条(1000)
// bindAddress 					- 本地监听的IP和端口(0.0.0.0:10001)
// dataDir 						- 数据持久化的目录("raft")
// join 						- 是否为新增的节点(false)
// members 						- 需要初始化节点内的成员列表(1:"0.0.0.0:10001")
func New(replicaID, shardID, electionRTT, heartbeatRTT, rttMillisecond, snapshotEntries, compactionOverhead uint64, bindAddress, dataDir string, join bool, members map[uint64]string) *RaftService {
	srv := &RaftService{replicaID: replicaID, shardID: shardID, join: join, members: members}

	srv.raftConfig = config.Config{
		ReplicaID:          replicaID,
		ShardID:            shardID,
		CheckQuorum:        true,
		ElectionRTT:        electionRTT,
		HeartbeatRTT:       heartbeatRTT,
		SnapshotEntries:    snapshotEntries,
		CompactionOverhead: compactionOverhead,
	}

	srv.stateMachine = &stateMachine{srv}

	srv.nhEvent = &nodeHostEvent{srv}

	srv.event = &raftListener{srv}

	dir := filepath.Join(dataDir, fmt.Sprintf("%d", replicaID))

	srv.hostConfig = config.NodeHostConfig{
		WALDir:              dir,
		NodeHostDir:         dir,
		RTTMillisecond:      rttMillisecond,
		RaftAddress:         bindAddress,
		SystemEventListener: srv.nhEvent,
		RaftEventListener:   srv.nhEvent,
	}

	return srv
}

// Start 启动Raft服务
func (rs *RaftService) Start() (err error) {
	rs.raft, err = dragonboat.NewNodeHost(rs.hostConfig)
	if err != nil {
		return err
	}

	return rs.raft.StartReplica(rs.members, rs.join, rs.stateMachine.create, rs.raftConfig)
}

// Stop 停止Raft服务
func (rs *RaftService) Stop() {
	if rs.raft == nil {
		return
	}

	rs.raft.Close()

	rs.raft = nil
}

// Set 从Raft集群内设置KV
// 并不是Leader才可以发起,集群内部任意角色都可以
func (rs *RaftService) Set(timeout time.Duration, key, value string) error {
	if rs.raft == nil {
		return fmt.Errorf("raft was nil")
	}

	if len(key) <= 0 {
		return fmt.Errorf("key length must>0")
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	// to learn
	session := rs.raft.GetNoOPSession(rs.shardID)
	val := &raftKV{key, value}

	bytes, err := json.Marshal(val)
	if err != nil {
		return err
	}

	_, err = rs.raft.SyncPropose(ctx, session, bytes)
	return err
}

// Get 从Raft集群内获取KV
// 当前使用的是线性一致性读
// 好处是保证一致性的前提下减缓Leader的读数据的IO压力
func (rs *RaftService) Get(timeout time.Duration, key string) (value string, err error) {
	if rs.raft == nil {
		return "", fmt.Errorf("raft was nil")
	}

	if len(key) <= 0 {
		return "", fmt.Errorf("key length must>0")
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	data, err := rs.raft.SyncRead(ctx, rs.shardID, key)

	if err != nil {
		return "", err
	}

	switch result := data.(type) {
	case string:
		return result, nil
	case []byte:
		return string(result), nil
	default:
		return "", fmt.Errorf("value data error, key: %v, value: %v", key, data)
	}
}

// TransferLeader 切换目标节点为Leader
// 建议通过Leader发起(Follower也可以,只不过内部会多一次请求)
func (rs *RaftService) TransferLeader(timeout time.Duration, targetReplicaID uint64) error {
	if rs.raft == nil {
		return fmt.Errorf("raft was nil")
	}

	err := rs.raft.RequestLeaderTransfer(rs.shardID, targetReplicaID)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	defer cancel()

	var leaderId uint64

	for leaderId != targetReplicaID {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			leaderId, _, _, err = rs.raft.GetLeaderID(rs.shardID)
		}
	}

	if err == nil && leaderId != targetReplicaID {
		err = fmt.Errorf("leader transfer fail, want: %d, current: %d", targetReplicaID, leaderId)
	}

	return err
}

// AddReplica 从现有集群中新增一个节点
// 被增加的节点New函数的join属性需设置true
// 被增加的节点要在集群内唯一,如果重复了会报错(即使删掉了在重新加也不行 - Raft规范)
func (rs *RaftService) AddReplica(timeout time.Duration, targetReplicaID uint64, targetAddress string) error {
	if rs.raft == nil {
		return fmt.Errorf("raft was nil")
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return rs.raft.SyncRequestAddReplica(ctx, rs.shardID, targetReplicaID, targetAddress, 0)
}

// DeleteReplica 从现有集群中删除一个节点
// 被删除的节点是永久性删除,不可以重复添加(Raft规范)
func (rs *RaftService) DeleteReplica(timeout time.Duration, targetReplicaID uint64) error {
	if rs.raft == nil {
		return fmt.Errorf("raft was nil")
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return rs.raft.SyncRequestDeleteReplica(ctx, rs.shardID, targetReplicaID, 0)
}

// Ready 集群是否准备完成
// 因为Raft节点之间是需要选举和通信的,所以Service.Start之后需要等待Ready后才可以正常使用
func (rs *RaftService) Ready(timeout time.Duration) (leaderID uint64, isLeader bool, err error) {
	if rs.raft == nil {
		return 0, false, fmt.Errorf("raft was nil")
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	defer cancel()

	var valid bool

	for !valid {
		select {
		case <-ctx.Done():
			return leaderID, isLeader, ctx.Err()
		default:
			leaderID, _, valid, err = rs.raft.GetLeaderID(rs.shardID)
		}
	}

	if rs.join {
		valid = false
		for !valid {
			select {
			case <-ctx.Done():
				return leaderID, leaderID == rs.replicaID, ctx.Err()
			default:
				info := rs.raft.GetNodeHostInfo(dragonboat.DefaultNodeHostInfoOption)
				for _, nodeInfo := range info.ShardInfoList {
					if nodeInfo.ShardID != rs.shardID {
						continue
					}
					valid = len(nodeInfo.Nodes) > 0
				}
			}
		}
	}

	return leaderID, leaderID == rs.replicaID, err
}

// NodeInfo 获取集群信息
func (rs *RaftService) NodeInfo() *dragonboat.NodeHostInfo {
	if rs.raft == nil {
		return nil
	}

	return rs.raft.GetNodeHostInfo(dragonboat.DefaultNodeHostInfoOption)
}

// ReplicaID 获取节点ID
// 集群内节点间ID需唯一
func (rs *RaftService) ReplicaID() uint64 {
	return rs.replicaID
}

// ShardID 获取集群ID
// 同一个集群下通过多个节点ID组成一个Raft环
func (rs *RaftService) ShardID() uint64 {
	return rs.shardID
}

// RegisterEvent 注册一个监听事件
// eventName - 事件名称
// handler - 回调函数
func (rs *RaftService) RegisterEvent(eventName string, handler EventHandler) error {
	if eventName == "" || handler == nil {
		return fmt.Errorf("event name or handler was nil")
	}
	rs.registerEvents.Store(fmt.Sprintf("%s%s", InnerEventMessage, eventName), handler)
	return nil
}

// Publish 向集群内广播数据(异步)
// eventName - 事件名称
// data - 事件消息内容(推荐用JSON串)
// replicaIDs - 需要接收的replicaID列表
// all - 忽略replicaIDs并广播到全部节点
func (rs *RaftService) Publish(timeout time.Duration, eventName, data string, replicaIDs []uint64, all bool) error {
	if len(replicaIDs) == 0 && !all {
		return fmt.Errorf("ReplicaIDs was nil")
	}
	m := &raftMsg{rs.ReplicaID(), replicaIDs, data, all, timeout, ""}
	bytes, _ := json.Marshal(m)
	return rs.Set(timeout, fmt.Sprintf("%s%s", InnerEventMessage, eventName), string(bytes))
}

// Request 向集群内广播数据(同步)
// eventName - 事件名称
// data - 事件消息内容(推荐用JSON串)
// replicaID - 需要接收的replicaID
func (rs *RaftService) Request(timeout time.Duration, eventName, data string, replicaID uint64) (value string, err error) {
	if rs.ReplicaID() == replicaID {
		return "", fmt.Errorf("Send Message To Self?")
	}
	m := &raftMsg{rs.ReplicaID(), []uint64{replicaID}, data, false, timeout, uuid.New().String()}
	bytes, _ := json.Marshal(m)

	responseKey := fmt.Sprintf("%s%s", InnerEventResponse, m.MsgID)
	rs.registerEvents.Store(responseKey, m.MsgID)
	defer rs.registerEvents.Delete(responseKey)

	err = rs.Set(timeout, fmt.Sprintf("%s%s", InnerEventMessage, eventName), string(bytes))
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ch := make(chan string)
	rs.responseMessage.Store(m.MsgID, ch)
	defer rs.responseMessage.Delete(m.MsgID)

	select {
	case <-ctx.Done():
		err = ctx.Err()
		return
	case value = <-ch:
		return value, err
	}
}
