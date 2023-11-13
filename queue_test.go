package queue

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"sync"
	"testing"
	"time"
)

type TestRequest struct {
	number int
}

type TestResponse struct {
	newNumber int
}

type TestExecutor struct {
	numRuns int
	wg      *sync.WaitGroup
}

func (e *TestExecutor) Execute(ctx context.Context, batch []*KeyedRequest[TestRequest], responses chan *KeyedResponse[TestResponse]) error {
	if e.wg != nil {
		e.wg.Done()
		e.wg.Wait()
	}
	e.numRuns++
	for _, i := range batch {
		response := &TestResponse{newNumber: i.Request.number + 2}
		responses <- &KeyedResponse[TestResponse]{
			ID:       i.ID,
			Response: response,
		}
	}
	return nil
}

func TestPriority_Enqueue(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)

	tracer := otel.GetTracerProvider().Tracer("test")

	t.Run("batch size 1 success", func(t *testing.T) {
		testConfig := PriorityConfig{
			BufferSize:   1,
			BatchSize:    1,
			WaitDuration: time.Millisecond * 10,
			PoolConfig: PoolConfig{
				ExecutionTimeout: time.Second * 10,
				BaseWorkers:      1,
			},
		}
		testExecutor := &TestExecutor{}
		queue := NewPriority[TestRequest, TestResponse](testConfig, testExecutor, tracer)

		response, err := queue.Enqueue(context.Background(), &TestRequest{number: 2}, High)
		r.NoError(err)
		a.Equal(&TestResponse{newNumber: 4}, response)
		a.Equal(1, testExecutor.numRuns)
	})

	t.Run("batch size 2 success", func(t *testing.T) {
		testConfig := PriorityConfig{
			BufferSize:   2,
			BatchSize:    2,
			WaitDuration: time.Second * 2,
			PoolConfig: PoolConfig{
				ExecutionTimeout: time.Second * 10,
				BaseWorkers:      1,
			},
		}
		testExecutor := &TestExecutor{}
		queue := NewPriority[TestRequest, TestResponse](testConfig, testExecutor, tracer)
		wg := sync.WaitGroup{}
		wg.Add(2)
		go func() {
			response, err := queue.Enqueue(context.Background(), &TestRequest{number: 2}, High)
			r.NoError(err)
			a.Equal(&TestResponse{newNumber: 4}, response)
			wg.Done()
		}()
		go func() {
			response, err := queue.Enqueue(context.Background(), &TestRequest{number: 1}, High)
			r.NoError(err)
			a.Equal(&TestResponse{newNumber: 3}, response)
			wg.Done()
		}()

		wg.Wait()

		a.Equal(1, testExecutor.numRuns)
	})

	t.Run("2 reqs batch size 1 results in 2 executions", func(t *testing.T) {
		testConfig := PriorityConfig{
			BufferSize:   2,
			BatchSize:    1,
			WaitDuration: time.Second * 2,
			PoolConfig: PoolConfig{
				ExecutionTimeout: time.Second * 10,
				BaseWorkers:      1,
			},
		}
		testExecutor := &TestExecutor{}
		queue := NewPriority[TestRequest, TestResponse](testConfig, testExecutor, tracer)
		wg := sync.WaitGroup{}
		wg.Add(2)
		go func() {
			response, err := queue.Enqueue(context.Background(), &TestRequest{number: 2}, High)
			r.NoError(err)
			a.Equal(&TestResponse{newNumber: 4}, response)
			wg.Done()
		}()
		go func() {
			response, err := queue.Enqueue(context.Background(), &TestRequest{number: 1}, High)
			r.NoError(err)
			a.Equal(&TestResponse{newNumber: 3}, response)
			wg.Done()
		}()

		wg.Wait()

		a.Equal(2, testExecutor.numRuns)
	})

	t.Run("10 reqs batch size 2 results in 5 executions", func(t *testing.T) {
		testConfig := PriorityConfig{
			BufferSize:   2,
			BatchSize:    2,
			WaitDuration: time.Second * 2,
			PoolConfig: PoolConfig{
				ExecutionTimeout: time.Second * 10,
				BaseWorkers:      1,
			},
		}
		testExecutor := &TestExecutor{}
		queue := NewPriority[TestRequest, TestResponse](testConfig, testExecutor, tracer)
		wg := sync.WaitGroup{}
		for x := 0; x < 10; x++ {
			x := x
			wg.Add(1)
			go func() {
				response, err := queue.Enqueue(context.Background(), &TestRequest{number: x}, High)
				r.NoError(err)
				a.Equal(&TestResponse{newNumber: x + 2}, response)
				wg.Done()
			}()
		}

		wg.Wait()
		a.Equal(5, testExecutor.numRuns)
	})

	t.Run("2 parallel workers", func(t *testing.T) {
		testConfig := PriorityConfig{
			BufferSize:   2,
			BatchSize:    1,
			WaitDuration: time.Second * 2,
			PoolConfig: PoolConfig{
				ExecutionTimeout: time.Second * 10,
				BaseWorkers:      2,
			},
		}
		exWg := sync.WaitGroup{}
		exWg.Add(2)
		testExecutor := &TestExecutor{wg: &exWg}
		queue := NewPriority[TestRequest, TestResponse](testConfig, testExecutor, tracer)
		wg := sync.WaitGroup{}
		for x := 0; x < 2; x++ {
			x := x
			wg.Add(1)
			go func() {
				response, err := queue.Enqueue(context.Background(), &TestRequest{number: x}, High)
				r.NoError(err)
				a.Equal(&TestResponse{newNumber: x + 2}, response)
				wg.Done()
			}()
		}

		wg.Wait()
		a.Equal(2, testExecutor.numRuns)
	})

	t.Run("10 parallel workers", func(t *testing.T) {
		testConfig := PriorityConfig{
			BufferSize:   2,
			BatchSize:    2,
			WaitDuration: time.Second * 2,
			PoolConfig: PoolConfig{
				ExecutionTimeout: time.Second * 10,
				BaseWorkers:      10,
			},
		}
		exWg := sync.WaitGroup{}
		exWg.Add(10)
		testExecutor := &TestExecutor{wg: &exWg}
		queue := NewPriority[TestRequest, TestResponse](testConfig, testExecutor, tracer)
		wg := sync.WaitGroup{}
		for x := 0; x < 20; x++ {
			x := x
			wg.Add(1)
			go func() {
				response, err := queue.Enqueue(context.Background(), &TestRequest{number: x}, High)
				r.NoError(err)
				a.Equal(&TestResponse{newNumber: x + 2}, response)
				wg.Done()
			}()
		}

		wg.Wait()
		a.Equal(10, testExecutor.numRuns)
	})
}
