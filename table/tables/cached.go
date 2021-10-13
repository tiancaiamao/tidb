package tables

import (
	"context"
	"fmt"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"time"
)

// TemporaryTableData is a interface to maintain temporary data in session
var _ table.Table = &cachedTable{}

// hotspotTableData is used for store hotspot table data

var _ table.CachedTable = &cachedTable{}

type cachedTable struct {
	TableCommon
	kv.MemBuffer
	*stateLocal
	stateRemote meta.StateRemote
	readCond    bool
	writeCond   bool
}


type stateLocal struct {
	lockType meta.CachedTableLockType
	lease    uint64
}

func (s *stateLocal) LockType() meta.CachedTableLockType {
	return s.lockType
}

func (s *stateLocal) Lease() uint64 {
	return s.lease
}
func (c *cachedTable) SetReadCondition(cond bool) {
	c.readCond = cond
}
func (c *cachedTable) GetReadCondition() bool {
	return c.readCond
}

func (c *cachedTable) CanReadFromCache(ts uint64) bool {
	return canReadFromCache(c.stateLocal, ts)
}

func (c *cachedTable) IsLocalStale(tsNow uint64) bool {
	return isLocalStale(c.stateLocal, tsNow)
}

func (c *cachedTable) SyncState(store kv.Storage) error {
	lockType, ts, err := c.stateRemote.Load(store)
	if err != nil {
		return err
	}
	c.stateLocal = &stateLocal{lockType: lockType, lease: ts}
	fmt.Println("sync state here", c.stateLocal)
	return nil
}

func isLocalStale(s *stateLocal, tsNow uint64) bool {
	if s == nil {
		fmt.Println("local is stale due to nil")
		return true
	}

	lease := s.Lease()
	if lease <= tsNow {
		fmt.Println("local is stale, lease = ", lease, "now = ", tsNow)
		return true
	}

	return false
}

func canReadFromCache(s *stateLocal, ts uint64) bool {
	if isLocalStale(s, ts) {
		return false
	}

	switch s.LockType() {
	case meta.CachedTableLockREAD:
		if s.Lease() > ts {
			//TODO : how to renew lease

			return true
		} else {
			// go lock for read
			return false
		}
	case meta.CachedTableLockNONE:
		// go lock for read
		return false
	case meta.CachedTableLockWRITE, meta.CachedTableLockINTENT:
		return false
	}
	panic("should never here")
}

func (c *cachedTable) PreLock(store kv.Storage,ts uint64) (uint64, error) {
	ts1 := oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(3 * time.Second))
	// Lock remote.
	oldLease, err := c.stateRemote.PreLock(store,ts, ts1)
	if err == nil {
		// Update local on success
		c.stateLocal = &stateLocal{
			lockType: meta.CachedTableLockINTENT,
			lease:    ts1,
		}
	}
	return oldLease, err
}

func (c *cachedTable) LockForRead(ctx sessionctx.Context, ts uint64) error {
	ts1 := oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(3 * time.Second))
	err := c.LoadData(ctx)
	if err != nil {
		return fmt.Errorf("reload data error")
	}
	err = c.stateRemote.LockForRead(ctx.GetStore(),ts, ts1)
	if err == nil {
		// Update the local state here on success.
		c.stateLocal = &stateLocal{
			lockType: meta.CachedTableLockREAD,
			lease:    ts1,
		}
	} else {
		fmt.Println("warn, lock for read get", err)
	}
	return nil
}
func isFirstRead(buffer kv.MemBuffer) bool {
	return buffer == nil
}

func (c *cachedTable) LockForWrite(store kv.Storage,ts uint64) error {
	// Make sure the local state is accurate.
	if c.IsLocalStale(ts) {
		err := c.SyncState(store)
		if err != nil {
			return err
		}
	}

	switch c.stateLocal.LockType() {
	case meta.CachedTableLockREAD:
		oldLease, err := c.PreLock(store,ts)
		if err != nil {
			return err
		}

		if c.stateLocal.Lease() > ts {
			// should wait read lease expire
			t1 := oracle.GetTimeFromTS(oldLease)
			t2 := oracle.GetTimeFromTS(ts)
			d := t1.Sub(t2)
			fmt.Println("lease =", t1, "now = ", t2, "sleep = ", d)
			time.Sleep(d)
		}
		if err := c.stateRemote.LockForWrite(store,ts); err != nil {
			return err
		}
	case meta.CachedTableLockNONE:
		// 在事物里写锁
		// lease 没意义
		// if 提交冲突了 那就重新走write CONdition
		if err := c.stateRemote.LockForWrite(store,ts); err != nil {
			// TODO if ERR = retry, goto to LockForWrite() again.
			return err
		}
	case meta.CachedTableLockWRITE:
		if c.stateLocal.Lease() > ts {
			fmt.Println("hold write lock, write directly")
			break
		}
		fmt.Println("write lock but lease is gone ...", c.stateLocal.Lease(), ts)
		// TODO: the whole steps
	}
	return nil
}

func (c *cachedTable) GetMemCached() kv.MemBuffer {
	return c.MemBuffer
}
func (c *cachedTable) GetFromMemCache(ctx context.Context, key kv.Key) ([]byte, error) {
	return c.MemBuffer.Get(ctx, key)
}
func (c *cachedTable) SetMemCache(key kv.Key, value []byte) error {
	return c.MemBuffer.Set(key, value)
}
func (c *cachedTable) IsFirstRead() bool {
	return isFirstRead(c.MemBuffer)
}

// NewCachedTable creates a new CachedTable Instance
func NewCachedTable(tbl *TableCommon) (table.Table, error) {
	return &cachedTable{
		TableCommon: *tbl,
		// only a remote instance but not really write to kv
		stateRemote: meta.NewStateRemoteHandle(tbl.tableID),
	}, nil

}

func (c *cachedTable) LoadData(ctx sessionctx.Context) error {
	prefix := tablecodec.GenTablePrefix(c.tableID)
	txn, err := ctx.Txn(true)
	if err != nil {
		return err
	}
	// 利用一个新的事物去new 一个空的membuffer然后load整个表从缓存表中
	buffTxn, err := ctx.GetStore().BeginWithOption(tikv.DefaultStartTSOption().SetStartTS(0))
	c.MemBuffer = buffTxn.GetMemBuffer() // empty buffer
	err = buffTxn.Commit(context.Background())
	if err != nil {
		return err
	}
	it, err := txn.Iter(prefix, prefix.PrefixNext())
	if err != nil {
		return err
	}
	defer it.Close()

	if !it.Valid() {
		return nil
	}
	for it.Valid() && it.Key().HasPrefix(prefix) {
		value := it.Value()
		err = c.SetMemCache(it.Key(), value)
		err := it.Next()
		if err != nil {
			return err
		}
	}

	return nil
}


// AddRecord implements the AddRecord method for the table.Table interface.
func (c *cachedTable) AddRecord(ctx sessionctx.Context, r []types.Datum, opts ...table.AddRecordOption) (recordID kv.Handle, err error) {
	txn, err := ctx.Txn(true)
	if err != nil {
		return nil, err
	}
	err = c.LockForWrite(ctx.GetStore(), txn.StartTS())
	if err != nil {
		return nil, err
	}

	record, err := c.TableCommon.AddRecord(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	return record, nil

}

// UpdateRecord implements table.Table
func (c *cachedTable) UpdateRecord(ctx context.Context, sctx sessionctx.Context, h kv.Handle, currData, newData []types.Datum, touched []bool) error {
	txn, err := sctx.Txn(true)
	if err != nil {
		return err
	}

	err = c.LockForWrite(sctx.GetStore(), txn.StartTS())
	if err != nil {
		return err
	}

	err = c.TableCommon.UpdateRecord(ctx, sctx, h, currData, newData, touched)
	if err != nil {
		return err
	}
	if err != nil {
		return err
	}
	return nil
}

// RemoveRecord implements table.Table RemoveRecord interface.
func (c *cachedTable) RemoveRecord(ctx sessionctx.Context, h kv.Handle, r []types.Datum) error {
	txn, err := ctx.Txn(true)
	if err != nil {
		return err
	}
	ts := txn.StartTS()

	err = c.LockForWrite(ctx.GetStore(), ts)
	if err != nil {
		return err
	}
	err = c.TableCommon.RemoveRecord(ctx, h, r)
	if err != nil {
		return err
	}
	return nil
}
