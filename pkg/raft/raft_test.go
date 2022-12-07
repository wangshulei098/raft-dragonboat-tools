package raft

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

func InitRaftCluster(dataDir string, replicaCount, shardID, electionRTT, heartbeatRTT uint64, join bool, startIndex uint64) []*RaftService {
	var raftServers []*RaftService

	var replicaIDs []uint64

	var members = make(map[uint64]string, replicaCount)

	var rttMillisecond = uint64(100)

	var snapshotEntries = uint64(0)

	var compactionOverhead = uint64(0)

	for i := uint64(startIndex); i < replicaCount+startIndex; i++ {
		replicaIDs = append(replicaIDs, i)
	}

	for _, val := range replicaIDs {
		members[val] = fmt.Sprintf("0.0.0.0:%d", shardID+val)
	}

	var joinMembers map[uint64]string

	if !join {
		joinMembers = members
	}

	for _, val := range replicaIDs {
		raftServers = append(raftServers, New(
			val,
			shardID,
			electionRTT,
			heartbeatRTT,
			rttMillisecond,
			snapshotEntries,
			compactionOverhead,
			members[val],
			dataDir,
			join,
			joinMembers,
		))
	}

	return raftServers
}

func TestNewRaft(t *testing.T) {
	dataDir := "raft"
	_ = os.RemoveAll(dataDir)
	defer func() {
		_ = os.RemoveAll(dataDir)
	}()

	var shardID = uint64(10000)

	var timeout = time.Second * 5

	servers := InitRaftCluster(dataDir, 3, shardID, 10, 3, false, 1)

	for _, val := range servers {
		if err := val.Start(); err != nil {
			t.Fatal(err)
		}
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(servers))

	var leaderID uint64
	var err error
	var checkLeaders []uint64
	var mu sync.Mutex

	for _, val := range servers {
		go func(server *RaftService) {
			defer wg.Done()
			if leaderID, _, err = server.Ready(timeout); err != nil {
				panic(err)
			}
			mu.Lock()
			checkLeaders = append(checkLeaders, leaderID)
			fmt.Println(leaderID, checkLeaders)
			mu.Unlock()
		}(val)
	}

	wg.Wait()

	for i := 0; i < len(checkLeaders)-1; i++ {
		if checkLeaders[i] != checkLeaders[i+1] {
			t.Fatal("leaderID id diff:", checkLeaders[i], checkLeaders[i+1])
		}
	}

	leader := leaderID - 1

	follower := leader + 1
	if follower >= uint64(len(servers)) {
		follower = 0
	}

	for _, server := range servers {
		server.Stop()
	}
}

func TestJoinAndRemoveRaft(t *testing.T) {
	dataDir := "raft"
	_ = os.RemoveAll(dataDir)
	defer func() {
		_ = os.RemoveAll(dataDir)
	}()

	var shardID = uint64(10000)

	var timeout = time.Second * 5

	servers := InitRaftCluster(dataDir, 3, shardID, 10, 3, false, 1)

	for _, val := range servers {
		if err := val.Start(); err != nil {
			t.Fatal(err)
		}
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(servers))

	for _, val := range servers {
		go func(server *RaftService) {
			defer wg.Done()
			if _, _, err := server.Ready(timeout); err != nil {
				panic(err)
			}
		}(val)
	}

	wg.Wait()

	newServers := InitRaftCluster(dataDir, 2, shardID, 10, 3, true, 4)
	for _, val := range newServers {
		servers[len(servers)-1].AddReplica(timeout, val.ReplicaID(), fmt.Sprintf("0.0.0.0:%d", shardID+val.ReplicaID()))
		if err := val.Start(); err != nil {
			t.Fatal(err)
		}
	}

	wg.Add(len(newServers))

	for _, val := range newServers {
		go func(server *RaftService) {
			defer wg.Done()
			if _, _, err := server.Ready(timeout); err != nil {
				panic(err)
			}
		}(val)
	}

	wg.Wait()

	if len(servers[len(servers)-1].NodeInfo().ShardInfoList[len(servers[len(servers)-1].NodeInfo().ShardInfoList)-1].Nodes) < 5 {
		t.Fatal("Nodes counts diff", "want", 5, "current", len(servers[len(servers)-1].NodeInfo().ShardInfoList[len(servers[len(servers)-1].NodeInfo().ShardInfoList)-1].Nodes))
	}

	for _, val := range newServers {
		if err := servers[len(servers)-1].DeleteReplica(timeout, val.ReplicaID()); err != nil {
			t.Fatal(err)
		}
		val.Stop()
	}

	if len(servers[len(servers)-1].NodeInfo().ShardInfoList[len(servers[len(servers)-1].NodeInfo().ShardInfoList)-1].Nodes) > 3 {
		t.Fatal("Nodes counts diff", "want", 3, "current", len(servers[len(servers)-1].NodeInfo().ShardInfoList[len(servers[len(servers)-1].NodeInfo().ShardInfoList)-1].Nodes))
	}

	for _, server := range servers {
		server.Stop()
	}
}

func TestOneGetOtherSet(t *testing.T) {
	dataDir := "raft"
	_ = os.RemoveAll(dataDir)
	defer func() {
		_ = os.RemoveAll(dataDir)
	}()

	var shardID = uint64(10000)

	var timeout = time.Second * 5

	servers := InitRaftCluster(dataDir, 3, shardID, 10, 3, false, 1)

	for _, val := range servers {
		if err := val.Start(); err != nil {
			t.Fatal(err)
		}
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(servers))

	for _, val := range servers {
		go func(server *RaftService) {
			defer wg.Done()
			if _, _, err := server.Ready(timeout); err != nil {
				panic(err)
			}
		}(val)
	}

	wg.Wait()

	want := "Value"
	if _, err := servers[len(servers)-1].Get(timeout, "Key"); err == nil {
		t.Fatal("key must be not exist")
	}

	if err := servers[len(servers)-1].Set(timeout, "Key", want); err != nil {
		t.Fatal(err)
	}

	if current, err := servers[len(servers)-2].Get(timeout, "Key"); err != nil || current != want {
		t.Fatal(err)
	}

	for _, server := range servers {
		server.Stop()
	}
}

func TestLeaders(t *testing.T) {
	dataDir := "raft"
	_ = os.RemoveAll(dataDir)
	defer func() {
		_ = os.RemoveAll(dataDir)
	}()

	var shardID = uint64(10000)

	var timeout = time.Second * 5

	servers := InitRaftCluster(dataDir, 3, shardID, 10, 3, false, 1)

	for _, val := range servers {
		if err := val.Start(); err != nil {
			t.Fatal(err)
		}
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(servers))

	var leaderID uint64
	var err error
	var checkLeaders []uint64
	var mu sync.Mutex

	for _, val := range servers {
		go func(server *RaftService) {
			defer wg.Done()
			if leaderID, _, err = server.Ready(timeout); err != nil {
				panic(err)
			}
			mu.Lock()
			checkLeaders = append(checkLeaders, leaderID)
			mu.Unlock()
		}(val)
	}

	wg.Wait()

	for i := 0; i < len(checkLeaders)-1; i++ {
		if checkLeaders[i] != checkLeaders[i+1] {
			t.Fatal("leaderID id diff:", checkLeaders[i], checkLeaders[i+1])
		}
	}

	leader := leaderID - 1

	follower := leader + 1
	if follower >= uint64(len(servers)) {
		follower = 0
	}

	err = servers[leader].TransferLeader(timeout, servers[follower].ReplicaID())
	if err != nil {
		if err != context.DeadlineExceeded {
			t.Fatal(err)
		}
		t.Log("leaderID transfer wait failed", err)
	}

	if newLeader, _, err := servers[follower].Ready(timeout); err != nil {
		t.Fatal(err)
	} else {
		if newLeader != servers[follower].ReplicaID() {
			t.Log("leaderID transfer failed warning", "current", newLeader, "want", servers[follower].ReplicaID())
		}
	}

	t.Log("leaderID transfer success")

	for _, server := range servers {
		server.Stop()
	}
}

func TestPublish(t *testing.T) {
	dataDir := "raft"
	_ = os.RemoveAll(dataDir)
	defer func() {
		_ = os.RemoveAll(dataDir)
	}()

	var shardID = uint64(10000)

	var timeout = time.Second * 5

	servers := InitRaftCluster(dataDir, 3, shardID, 10, 3, false, 1)

	for _, val := range servers {
		if err := val.Start(); err != nil {
			t.Fatal(err)
		}
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(servers))

	var leaderID uint64
	var err error
	var checkLeaders []uint64
	var mu sync.Mutex

	for _, val := range servers {
		go func(server *RaftService) {
			defer wg.Done()
			if leaderID, _, err = server.Ready(timeout); err != nil {
				panic(err)
			}
			mu.Lock()
			checkLeaders = append(checkLeaders, leaderID)
			fmt.Println(leaderID, checkLeaders)
			mu.Unlock()
		}(val)
	}

	wg.Wait()

	for i := 0; i < len(checkLeaders)-1; i++ {
		if checkLeaders[i] != checkLeaders[i+1] {
			t.Fatal("leaderID id diff:", checkLeaders[i], checkLeaders[i+1])
		}
	}

	leader := leaderID - 1

	follower := leader + 1
	if follower >= uint64(len(servers)) {
		follower = 0
	}

	eventNameA := "YinKingA"
	eventNameB := "YinKingB"

	var msgs []uint64
	servers[0].RegisterEvent(eventNameA, func(replicaID uint64, eventName, data string) (response string) {
		fmt.Println("Sender:", replicaID, "Event:", eventName, "Data:", data)
		msgs = append(msgs, replicaID)
		return "Giao!!!"
	})

	servers[0].RegisterEvent(eventNameB, func(replicaID uint64, eventName, data string) (response string) {
		fmt.Println("Sender:", replicaID, "Event:", eventName, "Data:", data)
		msgs = append(msgs, replicaID)
		return "Giao!!!"
	})

	servers[1].RegisterEvent(eventNameB, func(replicaID uint64, eventName, data string) (response string) {
		fmt.Println("Sender:", replicaID, "Event:", eventName, "Data:", data)
		msgs = append(msgs, replicaID)
		return "Giao!!!"
	})

	err = servers[1].Publish(time.Hour, eventNameA, "Replica 1's Message", []uint64{servers[0].ReplicaID()}, false)
	if err != nil {
		t.Fatal(err)
	}

	err = servers[2].Publish(time.Hour, eventNameB, "Replica 1 and 2's Message", nil, true)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second * 5)

	if len(msgs) != 3 {
		t.Fail()
	}

	for _, server := range servers {
		server.Stop()
	}
}

func TestRequest(t *testing.T) {
	dataDir := "raft"
	_ = os.RemoveAll(dataDir)
	defer func() {
		_ = os.RemoveAll(dataDir)
	}()

	var shardID = uint64(10000)

	var timeout = time.Second * 5

	servers := InitRaftCluster(dataDir, 3, shardID, 10, 3, false, 1)

	for _, val := range servers {
		if err := val.Start(); err != nil {
			t.Fatal(err)
		}
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(servers))

	var leaderID uint64
	var err error
	var checkLeaders []uint64
	var mu sync.Mutex

	for _, val := range servers {
		go func(server *RaftService) {
			defer wg.Done()
			if leaderID, _, err = server.Ready(timeout); err != nil {
				panic(err)
			}
			mu.Lock()
			checkLeaders = append(checkLeaders, leaderID)
			fmt.Println(leaderID, checkLeaders)
			mu.Unlock()
		}(val)
	}

	wg.Wait()

	for i := 0; i < len(checkLeaders)-1; i++ {
		if checkLeaders[i] != checkLeaders[i+1] {
			t.Fatal("leaderID id diff:", checkLeaders[i], checkLeaders[i+1])
		}
	}

	leader := leaderID - 1

	follower := leader + 1
	if follower >= uint64(len(servers)) {
		follower = 0
	}

	var msg string

	wantMsg := "Giao"
	eventName := "YinKing"
	servers[0].RegisterEvent(eventName, func(replicaID uint64, eventName, data string) (response string) {
		fmt.Println("Sender:", replicaID, "Event:", eventName, "Data:", data)
		return wantMsg
	})

	msg, err = servers[1].Request(time.Hour, eventName, "Replica 1's Message", servers[0].ReplicaID())
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second * 5)

	if msg != wantMsg {
		t.Fail()
	}

	for _, server := range servers {
		server.Stop()
	}
}
