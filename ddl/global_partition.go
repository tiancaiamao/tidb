package ddl

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/meta"
)

func onCreateGlobalPartitionRule(t *meta.Meta, job *model.Job) (ver int64, err error) {
	rule := new(model.GlobalPartitionRule)
	if err := job.DecodeArgs(rule); err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	if err := t.CreateGlobalPartitionRule(rule); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	ver, err = updateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.State = model.JobStateDone
	job.SchemaState = model.StatePublic
	return ver, nil
}

func onDropGlobalPartitionRule(t *meta.Meta, job *model.Job) (ver int64, err error) {
	rule := new(model.GlobalPartitionRule)
	if err := job.DecodeArgs(rule); err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	if err := t.DropGlobalPartitionRule(rule.ID); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	ver, err = updateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.State = model.JobStateDone
	job.SchemaState = model.StatePublic
	return ver, nil
}
