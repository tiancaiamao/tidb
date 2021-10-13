package tables

import (
	"fmt"
	"context"
	// "github.com/pingcap/log"
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

type opType byte

const (
	ExpiredRLOCKINREAD opType = iota
	ExpiredWLOCKINREAD
	ExpiredWLOCKINWRITE
	CLEANWLOCKINWRITE
	RENEWREADLOCK
	NONE
)

type applyMsg struct {
	ts                  uint64
	op                  opType
	CachedTableLockMeta *meta.CachedTableLockMetaInfo
	txn                 kv.Transaction
}

type cachedTable struct {
	TableCommon
	//cachedTableData
	kv.MemBuffer
	msg                 applyMsg
	*stateLocal
	stateRemote
	// CachedTableLockMeta *meta.CachedTableLockMetaInfo
	applyCh             chan applyMsg
	readCond            bool
	writeCond           bool
}

type stateLocal struct {
	lockType meta.CachedTableLockType
	lease uint64
}

func (s *stateLocal) LockType() meta.CachedTableLockType {
	return s.lockType
}

func (s *stateLocal) Lease() uint64 {
	return s.lease
}

type stateRemote interface {
	Load() (stateLocal, error)
	LockForRead(now, ts uint64) error
	PreLock(now, ts uint64) (uint64, error)
	LockForWrite(ts uint64) error
}

func (c *cachedTable) SetWriteCondition(cond bool) {
	c.writeCond = cond
}

func (c *cachedTable) GetWriteCondition() bool {
	return c.writeCond
}

func (c *cachedTable) SetReadCondition(cond bool) {
	c.readCond = cond
}
func (c *cachedTable) GetReadCondition() bool {
	return c.readCond
}
func (c *cachedTable) ApplyUpdateLockMeta(flag bool) {
	if !flag || c.msg.op == RENEWREADLOCK {
		c.applyCh <- c.msg
	}
}

type LockMetaInfo struct {
	meta.CachedTableLockMetaInfo
	scanCount int
	nextKey   kv.Key
	err       error
}

func (c *cachedTable) UpdateWRLock(ctx sessionctx.Context) {
	var err error

	msg := c.msg

	fmt.Println("UpdateWRLock ... msg = ", msg)

	switch msg.op {
	// 这个事物在更新完就会给他提交了
	case ExpiredRLOCKINREAD, ExpiredWLOCKINREAD:
		err = c.updateForRead(msg.txn, ctx, msg.ts)
		if err == nil {
			break
		}
	case ExpiredWLOCKINWRITE:
		err = c.updateForWrite(msg.txn, msg.ts)
		if err == nil {
			break
		}
	case CLEANWLOCKINWRITE:
		info := meta.NewCachedTableLockMetaInfo(c.tableID, meta.CachedTableLockNONE, msg.ts)
		err := c.UpdateLockMetaInfo(msg.txn, ctx, info)
		if err != nil {
			return
		}
	case RENEWREADLOCK:
		toTs := oracle.GoTimeToTS(oracle.GetTimeFromTS(msg.ts).Add(3 * time.Second))
		info := meta.NewCachedTableLockMetaInfo(c.tableID, meta.CachedTableLockREAD, toTs)
		err := c.UpdateLockMetaInfo(msg.txn, ctx, info)
		if err != nil {
			return
		}
	default:
		break
	}

}

func (c *cachedTable) updateForWrite(txn kv.Transaction, ts uint64) error {
	// info := c.CachedTableLockMeta
	// toTS := oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(3 * time.Second))
	// if info == nil {
	// 	metaInfo, err := c.LoadLockMetaInfo(txn)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	info = metaInfo
	// }
	// if info.Lock == meta.CachedTableLockREAD {
	// 	if info.Lease > ts {
	// 		info.Lease = toTS
	// 	}
	// } else if info.Lock == meta.CachedTableLockNONE {
	// 	err := info.LockForWrite(toTS)
	// 	if err != nil {
	// 		return err
	// 	}
	// } else {
	// 	if info.Lease < ts {
	// 		// 过期写锁更新它
	// 		err := info.LockForWrite(toTS)
	// 		if err != nil {
	// 			return err
	// 		}
	// 	}
	// }
	// var err error
	// m := meta.NewMeta(txn)
	// err = m.SetCachedTableLockInfo(c.tableID, info)
	// if err != nil {
	// 	return err
	// }
	// c.CachedTableLockMeta = info
	// err = txn.Commit(context.Background())
	// if err != nil {
	// 	return err
	// }
	return nil
}

func (c *cachedTable) updateForRead(txn kv.Transaction, ctx sessionctx.Context, ts uint64) error {


	// info := c.CachedTableLockMeta
	// toTS := oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(3 * time.Second))
	// err := c.LoadData(ctx)
	// if err != nil {
	// 	return err
	// }

	// fmt.Println("updateForRead ... ts = ", ts, " new ts ==", toTS)

	// // 过期了
	// if info.Lock == meta.CachedTableLockREAD {
	// 	fmt.Println("updateForRead ... 过期了")
	// 	err := info.RenewLease(toTS)
	// 	if err != nil {
	// 		return err
	// 	}
	// } else if info.Lock == meta.CachedTableLockNONE {
	// 	err := info.LockForRead(toTS)
	// 	if err != nil {
	// 		return err
	// 	}
	// } else {
	// 	// 写锁则不能一定能读
	// 	if info.Lease < ts {
	// 		// 过期写锁更新它
	// 		err := info.RenewLease(toTS)
	// 		if err != nil {
	// 			return err
	// 		}
	// 	}
	// }
	// m := meta.NewMeta(txn)
	// err = m.SetCachedTableLockInfo(c.tableID, info)
	// if err != nil {
	// 	return err
	// }
	// err = txn.Commit(context.Background())
	// if err != nil {
	// 	return err
	// }

	// c.CachedTableLockMeta = info
	return nil
}


func (c *cachedTable) CanReadFromCache(ts uint64) bool {
	return canReadFromCache(c.stateLocal, ts)
}

func (c *cachedTable) IsLocalStale(tsNow uint64) bool {
	return isLocalStale(c.stateLocal, tsNow)
}

func (c *cachedTable) SyncState() error {
	s, err := c.stateRemote.Load()
	if err != nil {
		return err
	}
	c.stateLocal = &s
	fmt.Println("sync state here", c.stateLocal)
	return nil
}

func isLocalStale(s *stateLocal, tsNow uint64) bool {
	if s == nil {
		fmt.Println("local is stale due to nil")
		return true
	}

	lease := s.Lease()
	if  lease <= tsNow {
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

type stateRemoteHandle struct {
	lease uint64
	lockType meta.CachedTableLockType
}

func (h *stateRemoteHandle) Load() (stateLocal ,error) {
	return stateLocal{
		lockType : h.lockType,
		lease : h.lease,
	}, nil
}

func (h *stateRemoteHandle) LockForRead(now, ts uint64) error {

	// In the server side:
	switch h.lockType {
	case meta.CachedTableLockNONE:
		h.lockType = meta.CachedTableLockREAD
		h.lease = ts
	case meta.CachedTableLockREAD:
		h.lease = ts
	case meta.CachedTableLockWRITE, meta.CachedTableLockINTENT:
		if now > h.lease {
			// clear orphan lock
			h.lockType = meta.CachedTableLockREAD
			h.lease = ts
		} else {
			return fmt.Errorf("fail to lock for read, curr state = %v", h.lockType)
		}
	}
	return nil
}

func (h *stateRemoteHandle) PreLock(now, ts uint64) (uint64, error) {
	// In the server side:
	oldLease := h.lease
	if h.lockType == meta.CachedTableLockNONE {
		h.lockType = meta.CachedTableLockINTENT
		h.lease = ts
		return oldLease, nil
	}

	if h.lockType == meta.CachedTableLockREAD {
		h.lockType = meta.CachedTableLockINTENT
		h.lease = ts
		return oldLease, nil
	}

	return 0, fmt.Errorf("fail to add lock intent, curr state = %v %d %d", h.lockType, h.lease, now)
}


func (h *stateRemoteHandle) LockForWrite(ts uint64) error {
	if h.lockType == meta.CachedTableLockINTENT {
		h.lockType = meta.CachedTableLockWRITE
		h.lease = ts
		return nil
	}

	return fmt.Errorf("lock for write fail, lock intent is gone! %v", h.lockType)
}

func (h *cachedTable) PreLock(ts uint64) (uint64, error) {
	ts1 := oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(3*time.Second))
	// Lock remote.
	oldLease, err := h.stateRemote.PreLock(ts, ts1)
	if err == nil {
		// Update local on success
		h.stateLocal = &stateLocal{
			lockType: meta.CachedTableLockINTENT,
			lease: ts1,
		}
	}
	return oldLease, err
}

func (c *cachedTable) LockForRead(ts uint64) error {
	ts1 := oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(3*time.Second))

	err :=  c.stateRemote.LockForRead(ts, ts1)
	if err == nil {
		// Update the local state here on success.
		c.stateLocal = &stateLocal{
			lockType: meta.CachedTableLockREAD,
			lease: ts1,
		}
	} else {
		fmt.Println("warn, lock for read get", err)
	}
	return nil
}

// func (c *cachedTable) ReadCondition(ctx sessionctx.Context, ts uint64) (bool, error) {
// 	var err error
// 	var txn kv.Transaction
// 	// 先去看本地缓存 在获取事物
// 	info := c.CachedTableLockMeta
// 	if info == nil || info.Lease < ts {
// 		fmt.Println("in ReadCondition ... load from remote", info, ts)
// 		txn, err = ctx.GetStore().Begin()
// 		if err != nil {
// 			return false, err
// 		}
// 		info, err = c.LoadLockMetaInfo(txn)
// 		if err != nil {
// 			return false, err
// 		}
// 	}
// 	msg := applyMsg{op: NONE, ts: ts, txn: nil}
// 	switch info.Lock {
// 	case meta.CachedTableLockREAD:
// 		if info.Lease > ts {
// 			if info.Lease > oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(1500*time.Millisecond)) {
// 				fmt.Println("renew ... lease for read lock ...", info.Lease)
// 				msg.op = RENEWREADLOCK
// 				msg.ts = info.Lease
// 				if txn == nil {
// 					txn, err = ctx.GetStore().Begin()
// 					if err != nil {
// 						return false, err
// 					}
// 				}
// 				msg.txn = txn
// 				c.msg = msg
// 				c.UpdateWRLock(ctx)
// 			}
// 			return true, nil
// 		} else {
// 			if txn == nil {
// 				txn, err = ctx.GetStore().Begin()
// 				if err != nil {
// 					return false, err
// 				}
// 			}
// 			msg.txn = txn
// 			msg.op = ExpiredRLOCKINREAD
// 			c.msg = msg
// 			return false, nil
// 		}

// 	case meta.CachedTableLockNONE:
// 		if txn == nil {
// 			txn, err = ctx.GetStore().Begin()
// 			if err != nil {
// 				return false, err
// 			}
// 		}
// 		fmt.Println("ReadCondition table lock is NONE, so add read lock before continue...")
// 		err = c.updateForRead(txn, ctx, msg.ts)
// 		if err != nil {
// 			return false, err
// 		}
// 		return true, nil
// 	case meta.CachedTableLockWRITE, meta.CachedTableLockINTENT:
// 		if info.Lease < ts {
// 			// 清除写锁 + 读锁 读
// 			if txn == nil {
// 				txn, err = ctx.GetStore().Begin()
// 				if err != nil {
// 					return false, err
// 				}
// 			}
// 			msg.op = ExpiredWLOCKINREAD
// 			c.msg = msg
// 			c.UpdateWRLock(ctx)
// 			return true, nil
// 		}
// 		return false, nil
// 	default:
// 		log.Error("We only have three lock type")
// 	}
// 	return false, nil
// }

func (c *cachedTable) LockForWrite(ts uint64) error {
	// Make sure the local state is accurate.
	if c.IsLocalStale(ts) {
		err := c.SyncState()
		if err != nil {
			return err
		}
	}

	switch c.stateLocal.LockType() {
	case meta.CachedTableLockREAD:
		// lockintend 不让读锁续约 -》TODO：读续约
		// 状态 lock INTENT, lease ts + 3 //
		oldLease, err := c.PreLock(ts)
		if err != nil{
			return err
		}

		if c.stateLocal.Lease() > ts {
			// should wait read lease expire
			//  物理时间来sleep多久
			t1 := oracle.GetTimeFromTS(oldLease)
			t2 := oracle.GetTimeFromTS(ts)
			d := t1.Sub(t2)
			fmt.Println("lease =", t1, "now = ", t2, "sleep = ", d)
			time.Sleep(d)
		}
		if err  := c.stateRemote.LockForWrite(ts); err != nil {
			return err
		}
	case meta.CachedTableLockNONE:
		// 在事物里写锁
		// lease 没意义
		// if 提交冲突了 那就重新走write CONdition
		if err := c.stateRemote.LockForWrite(ts); err != nil {
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
	return c.MemBuffer == nil
}

// NewCachedTable creates a new CachedTable Instance
func NewCachedTable(tbl *TableCommon) (table.Table, error) {
	return &cachedTable{
		TableCommon: *tbl,
		applyCh: make(chan applyMsg, 8),
		msg: applyMsg{op: NONE},
		stateRemote: &stateRemoteHandle{
			lockType: meta.CachedTableLockNONE,
		},
	}, nil
}

func (c *cachedTable) LoadData(ctx sessionctx.Context) error {
	prefix := tablecodec.GenTablePrefix(c.tableID)
	txn, err := ctx.Txn(true)
	if err != nil {
		return err
	}
	if c.IsFirstRead() {
		info := meta.NewCachedTableLockMetaInfo(c.tableID, meta.CachedTableLockNONE, txn.StartTS())
		err := c.UpdateLockMetaInfo(nil, ctx, info)
		if err != nil {
			return err
		}
	}
	// 利用一个新的事物去new 一个 空的membuffer 然后load整个表 从缓存表中
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

// load lockInfo from remote and set local
// func (c *cachedTable) LoadLockMetaInfo(txn kv.Transaction) (*meta.CachedTableLockMetaInfo, error) {
// 	m := meta.NewMeta(txn)
// 	info, err := m.GetCachedTableLockInfo(c.tableID)
// 	if err != nil {
// 		return nil, err
// 	}
// 	c.CachedTableLockMeta = info
// 	return info, nil
// }

// update lockMetaInfo remote and local
func (c *cachedTable) UpdateLockMetaInfo(txn kv.Transaction, ctx sessionctx.Context, info *meta.CachedTableLockMetaInfo) error {
	var err error
	if txn == nil {
		txn, err = ctx.GetStore().Begin()
		if err != nil {
			return err
		}
	}
	m := meta.NewMeta(txn)
	err = m.SetCachedTableLockInfo(c.tableID, info)
	if err != nil {
		return err
	}
	err = txn.Commit(context.Background())
	if err != nil {
		return err
	}
	// c.CachedTableLockMeta = info

	return nil
}

// AddRecord implements the AddRecord method for the table.Table interface.
func (c *cachedTable) AddRecord(ctx sessionctx.Context, r []types.Datum, opts ...table.AddRecordOption) (recordID kv.Handle, err error) {
	txn, err := ctx.Txn(true)
	if err != nil {
		return nil, err
	}
	err = c.LockForWrite(txn.StartTS())
	if err != nil {
		return nil, err
	}
	// newTxn, err := ctx.GetStore().Begin()
	// if err != nil {
	// 	return nil, err
	// }
	record, err := c.TableCommon.AddRecord(ctx, r, opts...)
	if err != nil {
		return nil, err
	}
	// c.msg = applyMsg{op: CLEANWLOCKINWRITE, ts: txn.StartTS(), txn:newTxn}
	// c.UpdateWRLock(ctx)
	return record, nil

}

// UpdateRecord implements table.Table
func (c *cachedTable) UpdateRecord(ctx context.Context, sctx sessionctx.Context, h kv.Handle, currData, newData []types.Datum, touched []bool) error {
	txn, err := sctx.Txn(true)
	if err != nil {
		return err
	}

	err = c.LockForWrite(txn.StartTS())
	if err != nil {
		return err
	}

	err = c.TableCommon.UpdateRecord(ctx, sctx, h, currData, newData, touched)
	if err != nil {
		return err
	}
	// newTxn, err := sctx.GetStore().Begin()
	// if err != nil {
	// 	return err
	// }
	// c.msg = applyMsg{op: CLEANWLOCKINWRITE, ts: txn.StartTS(), txn:newTxn}
	// c.UpdateWRLock(sctx)
	//err = c.UpdateForWrite(ctx, ts)
	if err != nil {
		return err
	}
	return nil
}

// RemoveRecord implements table.Table RemoveRecord interface.
func (c *cachedTable) RemoveRecord(ctx sessionctx.Context, h kv.Handle, r []types.Datum) error {
	//go c.UpdateWRLock(ctx)
	txn, err := ctx.Txn(true)
	if err != nil {
		return err
	}
	ts := txn.StartTS()

	err = c.LockForWrite(ts)
	if err != nil {
		return err
	}
	err = c.TableCommon.RemoveRecord(ctx, h, r)
	if err != nil {
		return err
	}
	// newTxn, err := ctx.GetStore().Begin()
	// if err != nil {
	// 	return err
	// }
	// c.msg = applyMsg{op: CLEANWLOCKINWRITE, ts:ts, txn:newTxn}
	// c.UpdateWRLock(ctx)
	//err = c.UpdateForWrite(ctx, ts)
	return nil
}
