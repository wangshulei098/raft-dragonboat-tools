package raft

import (
	"encoding/json"
	"fmt"
	"time"
)

// raftKV Raft内部KV数据,用于解析
type raftKV struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// raftMsg Raft消息
type raftMsg struct {
	ReplicaID        uint64        `json:"replicaID"`
	TargetReplicaIDs []uint64      `json:"targetReplicaIDs"`
	Body             string        `json:"body"`
	All              bool          `json:"all"`
	Timeout          time.Duration `json:"timeout"`
	MsgID            string        `json:"msg_id"`
}

// raftListener Raft事件监听器
type raftListener struct{ *RaftService }

// LogUpdated 日志已更新事件
func (rl *raftListener) LogUpdated(kv []byte, index uint64) {
	val := raftKV{}
	if err := json.Unmarshal(kv, &val); err != nil {
		panic(fmt.Sprintf("unmarshal raft kv error: %v, %s", err, string(kv)))
	}

	fmt.Println("LogUpdated", "ReplicaID", rl.ReplicaID(), "Key:", val.Key, "Value:", val.Value, "Index:", index)

	event, exists := rl.registerEvents.Load(val.Key)
	if !exists {
		return
	}

	var m raftMsg
	switch e := event.(type) {
	case EventHandler:
		{
			err := json.Unmarshal([]byte(val.Value), &m)
			if err != nil {
				fmt.Println("Unkonwn JSON Message Type:", "Key:", val.Key, "Value:", val.Value, "Err:", err)
				return
			}

			if m.ReplicaID == rl.ReplicaID() {
				return
			}

			if m.All {
				_ = e(m.ReplicaID, val.Key[len(InnerEventMessage):], m.Body)
			} else {
				for _, id := range m.TargetReplicaIDs {
					if id != rl.ReplicaID() {
						continue
					}
					m.Body = e(m.ReplicaID, val.Key[len(InnerEventMessage):], m.Body)
					response, _ := json.Marshal(m)
					if err = rl.Set(time.Second, fmt.Sprintf("%s%s", InnerEventResponse, m.MsgID), string(response)); err != nil {
						fmt.Println("Response Message Error:", err, "Message:", fmt.Sprintf("%s%s", InnerEventResponse, m.MsgID), "Body:", string(response))
					}
					break
				}
			}
		}
	case string:
		err := json.Unmarshal([]byte(val.Value), &m)
		if err != nil {
			fmt.Println("Unkonwn JSON Message Type:", "Key:", val.Key, "Value:", val.Value, "Err:", err)
			return
		}
		if val, ok := rl.responseMessage.Load(e); ok {
			ch, _ := val.(chan string)
			ch <- m.Body
		}
	default:
		fmt.Println("Unkonwn Message Type:", "Key:", val.Key, "Value:", val.Value)
		return
	}
}

// LogRead 日志读取事件
func (rl *raftListener) LogRead(key interface{}) {
	fmt.Println("LogRead", "Key:", key)
}

// LeaderUpdated Leader被更新事件
func (rl *raftListener) LeaderUpdated(leaderID, shardID, replicaID, term uint64) {
	fmt.Println("LeaderUpdated", "LeaderID:", leaderID, "ShardID:", shardID, "ReplicaID:", replicaID, "Term:", term)
}

// NodeShuttingDown 节点正在关闭事件
func (rl *raftListener) NodeShuttingDown() {
	fmt.Println("NodeShuttingdown")
}

// NodeUnloaded 节点被卸载事件
func (rl *raftListener) NodeUnloaded(replicaID, shardID uint64) {
	fmt.Println("NodeUnloaded", "ReplicaID:", replicaID, "ShardID:", shardID)
}

// NodeDeleted 节点被删除事件
func (rl *raftListener) NodeDeleted(replicaID, shardID uint64) {
	fmt.Println("NodeDeleted", "ReplicaID:", replicaID, "ShardID:", shardID)
}

// NodeReady 节点准备完成事件
func (rl *raftListener) NodeReady(replicaID, shardID uint64) {
	fmt.Println("NodeReady", "ReplicaID:", replicaID, "ShardID:", shardID)
}

// MembershipChanged 集群内成员发生变动事件
func (rl *raftListener) MembershipChanged(replicaID, shardID uint64) {
	fmt.Println("MembershipChanged", "ReplicaID:", replicaID, "ShardID:", shardID)
}

// ConnectionEstablished 网络连接已建立事件
func (rl *raftListener) ConnectionEstablished(address string, isSnapshot bool) {
	fmt.Println("ConnectionEstablished", "Address:", address, "IsSnapshot:", isSnapshot)
}

// ConnectionFailed 网络连接失败事件
func (rl *raftListener) ConnectionFailed(address string, isSnapshot bool) {
	fmt.Println("ConnectionFailed", "Address:", address, "IsSnapshot:", isSnapshot)
}
