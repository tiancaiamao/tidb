package meta

import (
	"context"
	"fmt"
	"github.com/pingcap/tidb/kv"
)

type CachedTableLockType byte

const (
	CachedTableLockNONE CachedTableLockType = iota
	CachedTableLockINTENT
	CachedTableLockREAD
	CachedTableLockWRITE
)

type StateRemote interface {
	Load(store kv.Storage) (CachedTableLockType, uint64, error)
	LockForRead(store kv.Storage, now, ts uint64) error
	PreLock(store kv.Storage,now, ts uint64) (uint64, error)
	LockForWrite( store kv.Storage,ts uint64) error
	//WriteToRemote(ctx sessionctx.Context) error
}

type StateRemoteHandle struct {
	tableID  int64
	lease    uint64
	LockType CachedTableLockType
	client   kv.Client
	txn      kv.Transaction
}

func NewStateRemoteHandle(tableID int64) StateRemote {
	return &StateRemoteHandle{
		tableID: tableID,
		LockType: CachedTableLockNONE,
	}

}
//func InitLockMetaInfo() error {
//
//}
func (h *StateRemoteHandle ) WriteToRemote(store kv.Storage) error{
	if h.txn == nil {
		txn, err := store.Begin()
		if err != nil {
			return  fmt.Errorf("start a new txn error")
		}
		h.txn = txn
	}

	meta := NewMeta(h.txn)
	err := meta.SetCachedTableLockInfo(h.tableID, &StateRemoteHandle{tableID: h.tableID, LockType: h.LockType, lease: h.lease})
	if err != nil {
		return  fmt.Errorf("set remote lock info error")
	}
	return nil
}
func (h *StateRemoteHandle) Load(store kv.Storage) (CachedTableLockType, uint64, error) {
	txn, err := store.Begin()
	if err != nil {
		return CachedTableLockNONE, 0, fmt.Errorf("start a new txn error")
	}
	meta := NewMeta(txn)
	info, err := meta.GetCachedTableLockInfo(h.tableID)
	if err != nil {
		return CachedTableLockNONE, 0, fmt.Errorf("get state from tikv error")
	}
	h.LockType = info.LockType
	h.lease = info.lease
	err = txn.Commit(context.Background())
	if err != nil {
		return CachedTableLockNONE, 0, fmt.Errorf("remote state modify txn commit error")
	}
	return h.LockType, h.lease, nil

}

func (h *StateRemoteHandle) LockForRead(store kv.Storage, now, ts uint64) error {

	// In the server side:
	switch h.LockType {
	case CachedTableLockNONE:

		h.LockType = CachedTableLockREAD
		h.lease = ts
	case CachedTableLockREAD:
		h.lease = ts
	case CachedTableLockWRITE, CachedTableLockINTENT:
		if now > h.lease {
			// clear orphan lock
			h.LockType = CachedTableLockREAD
			h.lease = ts
		} else {
			return fmt.Errorf("fail to lock for read, curr state = %v", h.LockType)
		}
	}
	err := h.WriteToRemote(store)
	if err != nil {
		return fmt.Errorf("fail to wirte lock info {LockType : %v Lease : %d } to remote", h.LockType, h.lease)
	}
	return nil
}

func (h *StateRemoteHandle) PreLock(store kv.Storage, now, ts uint64) (uint64, error) {
	// In the server side:
	oldLease := h.lease
	if h.LockType == CachedTableLockNONE {
		h.LockType = CachedTableLockINTENT
		h.lease = ts
		err := h.WriteToRemote(store)
		if err != nil {
			return 0, fmt.Errorf("fail to wirte lock info {LockType : %v Lease : %d } to remote", h.LockType, h.lease)
		}
		return oldLease, nil

	}

	if h.LockType == CachedTableLockREAD {
		h.LockType = CachedTableLockINTENT
		h.lease = ts
		err := h.WriteToRemote(store)
		if err != nil {
			return 0, fmt.Errorf("fail to wirte lock info {LockType : %v Lease : %d } to remote", h.LockType, h.lease)
		}
		return oldLease, nil
	}
	return 0, fmt.Errorf("fail to add lock intent, curr state = %v %d %d", h.LockType, h.lease, now)
}

func (h *StateRemoteHandle) LockForWrite(store kv.Storage, ts uint64) error {
	if h.LockType == CachedTableLockINTENT || h.LockType == CachedTableLockNONE {
		h.LockType = CachedTableLockWRITE
		h.lease = ts
		err := h.WriteToRemote(store)
		if err != nil {
			return fmt.Errorf("fail to wirte lock info {LockType : %v Lease : %d } to remote", h.LockType, h.lease)
		}
		return nil
	}

	return fmt.Errorf("lock for write fail, lock intent is gone! %v", h.LockType)
}
