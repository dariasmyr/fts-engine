package workers

import "context"

type Job struct {
	Description JobDescriptor
	ExecFn      ExecutionFn
	Args        interface{}
}

type ExecutionFn func(ctx context.Context, args interface{}) (interface{}, error)

type JobID string
type jobType string
type jobMetadata map[string]interface{}

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
	value, err := j.ExecFn(ctx, j.Args)
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
