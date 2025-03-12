package workers

import (
	"context"
	"fts-hw/internal/domain/models"
)

type Job struct {
	Description JobDescriptor
	ExecFn      ExecutionFn
	Args        *models.Document
}

type ExecutionFn func(ctx context.Context, args models.Document) (string, error)

type JobID string
type jobType string
type jobMetadata map[string]models.Meta

type JobDescriptor struct {
	ID       JobID
	JobType  jobType
	Metadata jobMetadata
}

type Result struct {
	Value       interface{}
	Err         error
	Description JobDescriptor
}

func (j Job) execute(ctx context.Context) Result {
	value, err := j.ExecFn(ctx, *j.Args)
	if err != nil {
		return Result{
			Err:         err,
			Description: j.Description,
		}
	}

	return Result{
		Value:       value,
		Description: j.Description,
	}
}
