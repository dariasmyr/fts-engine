package workers

import "context"

type Job[T interface{}] struct {
	Description JobDescriptor
	ExecFn      ExecutionFn[T]
	Args        T
}

type ExecutionFn[T interface{}] func(ctx context.Context, args T) (T, error)

type JobID string
type jobType string
type jobMetadata map[string]interface{}

type JobDescriptor struct {
	ID       JobID
	JobType  jobType
	Metadata jobMetadata
}

type Result[T interface{}] struct {
	Value       T
	Err         error
	Description JobDescriptor
}

func (j Job[T]) execute(ctx context.Context) Result[T] {
	value, err := j.ExecFn(ctx, j.Args)
	if err != nil {
		return Result{
			Err:         err,
			Description: j.Description,
		}
	}

	return Result[T]{
		Value:       value,
		Description: j.Description,
	}
}
