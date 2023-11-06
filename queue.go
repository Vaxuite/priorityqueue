package queue

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"golang.org/x/sync/semaphore"
	"sync"
	"time"
)

type Config struct {
	BufferSize   int64
	BatchSize    int
	WaitDuration time.Duration
}

type Level int

var (
	High Level = 0
	Low  Level = 1
)

type internalRequest[Request any] struct {
	request *Request
	id      uuid.UUID
}

type Executor[Request any, Response any] interface {
	Execute(ctx context.Context, batch []*Request) ([]*Response, error)
}

type Priority[Request any, Response any] struct {
	highPriority  chan *internalRequest[Request]
	lowPriority   chan *internalRequest[Request]
	resultsMap    map[uuid.UUID]func(result *Response, err error)
	totalEnqueued *semaphore.Weighted
	executor      Executor[Request, Response]
	config        Config
}

func NewPriority[Request any, Response any](config Config) *Priority[Request, Response] {
	p := &Priority[Request, Response]{
		highPriority:  make(chan *internalRequest[Request], config.BufferSize),
		lowPriority:   make(chan *internalRequest[Request], config.BufferSize),
		resultsMap:    map[uuid.UUID]func(result *Response, err error){},
		config:        config,
		totalEnqueued: semaphore.NewWeighted(config.BufferSize),
	}
	return p
}

func (p *Priority[Request, Response]) Enqueue(ctx context.Context, item *Request, level Level) (*Response, error) {
	if err := p.totalEnqueued.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	var returnedResponse *Response
	var returnedError error
	wg := sync.WaitGroup{}
	id := uuid.New()
	p.resultsMap[id] = func(response *Response, err error) {
		returnedResponse = response
		returnedError = err
		wg.Done()
	}
	request := &internalRequest[Request]{
		request: item,
		id:      id,
	}
	switch level {
	case High:
		p.highPriority <- request
	default:
		p.lowPriority <- request
	}
	wg.Add(1)
	// todo: replace with ctx
	wg.Wait()
	return returnedResponse, returnedError
}

func (p *Priority[Request, Response]) startWorkerPool() error {
	errChan := make(chan error)
	for {
		select {
		case err := <-errChan:
			return err
		default:
		}
		batch := p.makeBatch(p.config.BatchSize)
		if len(batch) > 0 {
			go func() {
				errChan <- p.executeBatch(batch)
			}()
		} else {
			// wait again as batch was empty
		}
	}
}

func (p *Priority[Request, Response]) executeBatch(batch []*internalRequest[Request]) error {
	var requestBatch []*Request
	for _, i := range batch {
		requestBatch = append(requestBatch, i.request)
	}
	// todo: better context
	responses, err := p.executor.Execute(context.Background(), requestBatch)
	if len(responses) != len(batch) {
		return fmt.Errorf("different number of responses to requests returned")
	}
	for i := 0; i < len(batch); i++ {
		p.resultsMap[batch[i].id](responses[i], err)
	}
	return nil
}

func (p *Priority[Request, Response]) makeBatch(size int) []*internalRequest[Request] {
	var batch []*internalRequest[Request]
	done := time.After(p.config.WaitDuration)
	for {
		if len(batch) == size {
			return batch
		}
		select {
		case <-done:
			return batch
		case item := <-p.highPriority:
			batch = append(batch, item)
			p.totalEnqueued.Release(1)
		case item := <-p.lowPriority:
			batch = append(batch, item)
			p.totalEnqueued.Release(1)
		}

	}
}
