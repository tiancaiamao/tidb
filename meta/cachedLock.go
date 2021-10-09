package meta

import (
	"github.com/pingcap/errors"
)

type CachedTableLockType byte

const (
	CachedTableLockNONE CachedTableLockType = iota
	CachedTableLockINTENT
	CachedTableLockREAD
	CachedTableLockWRITE
)

type StateRemote interface {
	LockForRead() error
	RenewLease() error
	LockForWrite() error
	WriteAndUnlock() error
	CleanOrphanLock() error
}
type CachedTableLockMetaInfo struct {
	TableID int64
	Lock    CachedTableLockType
	Lease   uint64
}

func NewCachedTableLockMetaInfo(ID int64, lock CachedTableLockType, lease uint64) *CachedTableLockMetaInfo {
	return &CachedTableLockMetaInfo{
		TableID: ID,
		Lock:    lock,
		Lease:   lease,
	}

}
func (h *CachedTableLockMetaInfo) LockForRead(lease uint64) error {

	if h.Lock == CachedTableLockWRITE {
		return errors.New("这种情况锁不能是写锁")
	}
	h.Lock = CachedTableLockREAD
	h.Lease = lease
	return nil
}

func (h *CachedTableLockMetaInfo) RenewLease(lease uint64) error {

	if h.Lock != CachedTableLockREAD {
		return errors.New("这种情况锁必须是读锁")
	}
	h.Lease = lease
	return nil
}
func (h *CachedTableLockMetaInfo) LockForWrite(lease uint64) error {
	h.Lock = CachedTableLockWRITE
	h.Lease = lease
	return nil
}
func (h *CachedTableLockMetaInfo) WriteAndUnlock() error {
	// TODO: 不需要写了释放锁？
	return nil
}
func (h *CachedTableLockMetaInfo) CleanOrphanLock(lease uint64) error {
	if h.Lease < lease {
		return errors.New("lease 不符合要求")
	}
	if h.Lock != CachedTableLockWRITE {
		return errors.New("必须是写锁")
	}
	h.Lock = CachedTableLockNONE
	return nil
}
