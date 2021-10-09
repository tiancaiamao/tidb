package tables

import (
	"context"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
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
	CachedTableLockMeta *meta.CachedTableLockMetaInfo
	applyCh             chan applyMsg
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
	for {
		msg := <-c.applyCh
		switch msg.op {
		// 这个事物在更新完就会给他提交了
		case ExpiredRLOCKINREAD, ExpiredWLOCKINREAD:
			err = c.updateForRead(msg.txn, ctx, msg.ts)
			if err == nil {
				break
			}
		case  ExpiredWLOCKINWRITE:
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
			info := meta.NewCachedTableLockMetaInfo(c.tableID, meta.CachedTableLockREAD, msg.ts)
			err := c.UpdateLockMetaInfo(msg.txn, ctx, info)
			if err != nil {
				return
			}
		default:
			break
		}
		break
	}
}

func (c *cachedTable) updateForWrite(txn kv.Transaction, ts uint64) error {
	info := c.CachedTableLockMeta
	toTS := oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(3 * time.Second))
	if info == nil {
		metaInfo, err := c.LoadLockMetaInfo(txn)
		if err != nil {
			return err
		}
		info = metaInfo
	}
	if info.Lock == meta.CachedTableLockREAD {
		if info.Lease > ts {
			info.Lease = toTS
		}
	} else if info.Lock == meta.CachedTableLockNONE {
		err := info.LockForWrite(toTS)
		if err != nil {
			return err
		}
	} else {
		if info.Lease < ts {
			// 过期写锁更新它
			err := info.LockForWrite(toTS)
			if err != nil {
				return err
			}
		}
	}
	var err error
	m := meta.NewMeta(txn)
	err = m.SetCachedTableLockInfo(c.tableID, info)
	if err != nil {
		return err
	}
	c.CachedTableLockMeta = info
	err = txn.Commit(context.Background())
	if err != nil {
		return err
	}
	return nil
}

func (c *cachedTable) updateForRead(txn kv.Transaction, ctx sessionctx.Context, ts uint64) error {

	info := c.CachedTableLockMeta
	toTS := oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(3 * time.Second))
	err := c.LoadData(ctx)
	if err != nil {
		return err
	}
	// 过期了
	if info.Lock == meta.CachedTableLockREAD {
		err := info.RenewLease(toTS)
		if err != nil {
			return err
		}
	} else if info.Lock == meta.CachedTableLockNONE {
		err := info.LockForRead(toTS)
		if err != nil {
			return err
		}
	} else {
		// 写锁则不能一定能读
		if info.Lease < ts {
			// 过期写锁更新它
			err := info.RenewLease(toTS)
			if err != nil {
				return err
			}
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

	c.CachedTableLockMeta = info
	return nil
}

func (c *cachedTable) ReadCondition(ctx sessionctx.Context, ts uint64) (bool, error) {
	var err error
	txn, err := ctx.GetStore().Begin()
	if err != nil {
		return false, err
	}
	info := c.CachedTableLockMeta
	if c.IsFirstRead() {
		err = txn.Commit(context.Background())
		if err != nil {
			return false, err
		}
		return true, nil
	}
	if info == nil {
		info, err = c.LoadLockMetaInfo(txn)
		if err != nil {
			return false, err
		}
	}
	msg := applyMsg{op: NONE, ts: ts, txn: txn}
	switch info.Lock {
	case meta.CachedTableLockREAD:
		if info.Lease > ts {
			err := txn.Commit(context.Background())
			if err != nil {
				return false, err
			}
			if info.Lease > oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(2*time.Second)) {
				msg.op = RENEWREADLOCK
				msg.ts = info.Lease
				c.msg = msg
			}
			return true, nil
		} else {
			msg.op = ExpiredRLOCKINREAD
			c.msg = msg
			return false, nil
		}

	case meta.CachedTableLockNONE:
		err = c.updateForRead(msg.txn, ctx, msg.ts)
		if err != nil {
			return false, err
		}
		return true, nil
	case meta.CachedTableLockWRITE, meta.CachedTableLockINTENT:
		if info.Lease < ts {
			// 清除写锁 + 读锁 读
			msg.op = ExpiredWLOCKINREAD
			c.msg = msg
			return true, nil
		}
		return false, nil
	default:
		log.Error("We only have three lock type")
	}
	return false, nil
}
func (c *cachedTable) WriteCondition(ctx sessionctx.Context, ts uint64) (bool, error) {
	var err error
	txn, err := ctx.GetStore().Begin()
	if err != nil {
		return false, err
	}
	info := c.CachedTableLockMeta
	// 读上来 在写 下去 要原子性
	// ？
	if info == nil {
		// 在一个事物里
		info, err = c.LoadLockMetaInfo(txn)
		if err != nil {
			return false, err
		}
	}
	msg := applyMsg{op: ExpiredWLOCKINWRITE, ts: ts, txn: txn}
	toTS := oracle.GoTimeToTS(oracle.GetTimeFromTS(ts).Add(3 * time.Second))
	newTs := oracle.GoTimeToTS(oracle.GetTimeFromTS(info.Lease).Add(3 * time.Second))
	switch info.Lock {
	case meta.CachedTableLockREAD:
		// lockintend 不让读锁续约 -》TODO：读续约
		readTs := info.Lease
		info.Lock = meta.CachedTableLockINTENT
		info.Lease = toTS
		err := c.UpdateLockMetaInfo(nil, ctx, info)
		if err != nil {
			return false, err
		}
		// info ->状态 lock INTENT, info.lease ts + 3 //
		// read.lease > ts
		if readTs > ts {
			// should wait read lease expire
			//  物理时间来sleep多久
			time.Sleep(time.Duration(oracle.GetTimeFromTS(readTs - ts).Second()))
		}
		info.Lock = meta.CachedTableLockWRITE
		info.Lease = newTs
		err = c.UpdateLockMetaInfo(nil, ctx, info)
		if err != nil {
			return false, err
		}
		// 写锁  read.lease + 3
		// 远程写 lock intent lease
		//
		return true, nil
	case meta.CachedTableLockNONE:
		// 在事物里写锁
		// lease 没意义
		// if 提交冲突了 那就重新走write CONdition

		err := c.updateForWrite(msg.txn, msg.ts)
		if err != nil {
			return false, err
		}

		return true, nil

	case meta.CachedTableLockWRITE:
		//  txn
		if info.Lease > ts {
			return true, nil // 可以的
		} else {
			c.msg = msg
			return false, nil //
		}
	default:
		log.Error("We only have three lock type")
	}
	return false, nil
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
	return &cachedTable{TableCommon: *tbl, applyCh: make(chan applyMsg, 8), msg: applyMsg{op: NONE}}, nil
}
func (c *cachedTable) LoadData(ctx sessionctx.Context) error {
	prefix := tablecodec.GenTablePrefix(c.tableID)
	txn, err := ctx.GetStore().Begin()
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
	defer func() {
		it.Close()
		err := txn.Commit(context.Background())
		if err != nil {
			log.Error("meet error and error is ")
		}
	}()
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
func (c *cachedTable) SetLockMetaInfo(info *meta.CachedTableLockMetaInfo) {
	c.CachedTableLockMeta = info
}

// load lockInfo from remote and set local
func (c *cachedTable) LoadLockMetaInfo(txn kv.Transaction) (*meta.CachedTableLockMetaInfo, error) {
	m := meta.NewMeta(txn)
	info, err := m.GetCachedTableLockInfo(c.tableID)
	if err != nil {
		return nil, err
	}
	c.CachedTableLockMeta = info
	return info, nil
}

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
	c.CachedTableLockMeta = info

	return nil
}

// AddRecord implements the AddRecord method for the table.Table interface.
func (c *cachedTable) AddRecord(ctx sessionctx.Context, r []types.Datum, opts ...table.AddRecordOption) (recordID kv.Handle, err error) {
	go c.UpdateWRLock(ctx)
	return cachedTableAddRecord(ctx, c, r, opts)
}
func cachedTableAddRecord(ctx sessionctx.Context, c *cachedTable, r []types.Datum, opts []table.AddRecordOption) (recordID kv.Handle, err error) {
	txn, err := ctx.Txn(true)
	if err != nil {
		return nil, err
	}
	cond, err := c.WriteCondition(ctx, txn.StartTS())
	if err != nil {
		return nil, err
	}
	if cond {
		record, err := c.TableCommon.AddRecord(ctx, r, opts...)
		if err != nil {
			return nil, err
		}
		c.msg = applyMsg{op: CLEANWLOCKINWRITE, ts: txn.StartTS(), txn: txn}
		if err != nil {
			return nil, err
		}
		return record, nil
	} else {
		c.ApplyUpdateLockMeta(cond)
	}

	return nil, errors.New("不满足写条件，不能写该表")
}

// UpdateRecord implements table.Table
func (c *cachedTable) UpdateRecord(ctx context.Context, sctx sessionctx.Context, h kv.Handle, currData, newData []types.Datum, touched []bool) error {
	go c.UpdateWRLock(sctx)
	return cachedTableUpdateRecord(ctx, sctx, c, h, currData, newData, touched)
}

func cachedTableUpdateRecord(gctx context.Context, ctx sessionctx.Context, c *cachedTable, h kv.Handle, currData, newData []types.Datum, touched []bool) error {

	txn, err := ctx.Txn(true)
	if err != nil {
		return err
	}
	cond, err := c.WriteCondition(ctx, txn.StartTS())
	if err != nil {
		return err
	}
	if cond {
		err := c.TableCommon.UpdateRecord(gctx, ctx, h, currData, newData, touched)
		if err != nil {
			return err
		}
		err = c.CachedTableLockMeta.CleanOrphanLock(txn.StartTS())
		if err != nil {
			return err
		}
		return nil
	}
	//err = c.UpdateForWrite(ctx, ts)
	if err != nil {
		return err
	}
	return nil
}

// RemoveRecord implements table.Table RemoveRecord interface.
func (c *cachedTable) RemoveRecord(ctx sessionctx.Context, h kv.Handle, r []types.Datum) error {
	go c.UpdateWRLock(ctx)
	txn, err := ctx.Txn(true)
	if err != nil {
		return err
	}
	ts := txn.StartTS()
	cond, err := c.WriteCondition(ctx, txn.StartTS())
	if err != nil {
		return err
	}
	if cond {
		err := c.TableCommon.RemoveRecord(ctx, h, r)
		if err != nil {
			return err
		}
		err = c.CachedTableLockMeta.CleanOrphanLock(ts)
		if err != nil {
			return err
		}
		return nil
	}
	//err = c.UpdateForWrite(ctx, ts)
	if err != nil {
		return err
	}
	return nil
}
