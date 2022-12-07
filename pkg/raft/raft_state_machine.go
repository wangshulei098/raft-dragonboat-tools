package raft

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/lni/dragonboat/v4/statemachine"
)

type stateMachine struct {
	*RaftService
}

func (sm *stateMachine) create(_, _ uint64) statemachine.IStateMachine {
	return sm
}

func (sm *stateMachine) Update(entry statemachine.Entry) (statemachine.Result, error) {
	if sm.closed.Load() {
		return statemachine.Result{}, fmt.Errorf("raft was closed")
	}
	sm.event.LogUpdated(entry.Cmd, entry.Index)
	val := &raftKV{}
	if err := json.Unmarshal(entry.Cmd, val); err != nil {
		return statemachine.Result{}, err
	}
	sm.kv.Store(val.Key, val.Value)

	return statemachine.Result{Value: uint64(len(entry.Cmd))}, nil
}

func (sm *stateMachine) Lookup(i interface{}) (interface{}, error) {
	if sm.closed.Load() {
		return nil, fmt.Errorf("raft was closed")
	}
	sm.event.LogRead(i)
	val, ok := sm.kv.Load(i.(string))
	if !ok {
		return nil, fmt.Errorf("key: %v not exists", i)
	}
	return val, nil
}

func (sm *stateMachine) SaveSnapshot(writer io.Writer, _ statemachine.ISnapshotFileCollection, _ <-chan struct{}) error {
	if sm.closed.Load() {
		return fmt.Errorf("raft was closed")
	}
	var lst []raftKV
	sm.kv.Range(func(key, value any) bool {
		lst = append(lst, raftKV{key.(string), value.(string)})
		return true
	})

	data, err := json.Marshal(lst)
	if err != nil {
		return err
	}

	_, err = writer.Write(data)

	return err
}

func (sm *stateMachine) RecoverFromSnapshot(reader io.Reader, _ []statemachine.SnapshotFile, _ <-chan struct{}) error {
	if sm.closed.Load() {
		return fmt.Errorf("raft was closed")
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}

	var lst []raftKV
	err = json.Unmarshal(data, &lst)
	if err != nil {
		return err
	}
	for _, val := range lst {
		sm.kv.Store(val.Key, val.Value)
	}
	return nil
}

func (sm *stateMachine) Close() error {
	if sm.closed.Load() {
		return fmt.Errorf("raft already closed")
	}
	sm.closed.Store(true)
	return nil
}
