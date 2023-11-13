package queue

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/marusama/semaphore"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"sync"
	"time"
)

type Config struct {
	BaseWorkers      int
	BatchSize        int
	BufferSize       int
	WaitDuration     time.Duration
	ExecutionTimeout time.Duration
	Name             string
}

type Level string

var (
	High Level = "high"
	Low  Level = "low"
)

type internalRequest[Request any, Response any] struct {
	request  *Request
	id       string
	response func(*Response, error)
	ctx      context.Context
}

type KeyedResponse[Response any] struct {
	ID       string
	Response *Response
}

type KeyedRequest[Request any] struct {
	ID      string
	Request *Request
}

type Executor[Request any, Response any] interface {
	Execute(ctx context.Context, batch []*KeyedRequest[Request], responses chan *KeyedResponse[Response]) error
}

type Priority[Request any, Response any] struct {
	highPriority chan *internalRequest[Request, Response]
	lowPriority  chan *internalRequest[Request, Response]
	executor     Executor[Request, Response]
	config       Config
	pool         semaphore.Semaphore
	tracer       trace.Tracer
}

func NewPriority[Request any, Response any](config Config, executor Executor[Request, Response], tracer trace.Tracer) *Priority[Request, Response] {
	p := &Priority[Request, Response]{
		highPriority: make(chan *internalRequest[Request, Response], config.BufferSize),
		lowPriority:  make(chan *internalRequest[Request, Response], config.BufferSize),
		config:       config,
		executor:     executor,
		tracer:       tracer,
	}

	p.pool = semaphore.New(p.config.BaseWorkers)
	go p.startWorkerPool()
	return p
}

func (p *Priority[Request, Response]) createExecutors(amount int) []func(ctx context.Context, batch []*KeyedRequest[Request], responses chan *KeyedResponse[Response]) error {
	var executors []func(ctx context.Context, batch []*KeyedRequest[Request], responses chan *KeyedResponse[Response]) error
	for x := 0; x < amount; x++ {
		executors = append(executors, p.executor.Execute)
	}
	return executors
}

func (p *Priority[Request, Response]) Enqueue(ctx context.Context, item *Request, level Level) (*Response, error) {
	ctx, span := p.tracer.Start(
		ctx,
		"PQueue - Enqueue",
		trace.WithAttributes(
			attribute.String("queue.name", p.config.Name),
			attribute.String("queue.priority", string(level)),
		),
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer span.End()
	var returnedResponse *Response
	var returnedError error
	wg := sync.WaitGroup{}
	id := uuid.New()
	request := &internalRequest[Request, Response]{
		request: item,
		id:      id.String(),
		ctx:     ctx,
		response: func(response *Response, err error) {
			returnedResponse = response
			returnedError = err
			wg.Done()
		},
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
			err := p.pool.Acquire(context.Background(), 1)
			if err != nil {
				return err
			}
			go func() {
				defer func() {
					p.pool.Release(1)
				}()
				if err := p.executeBatch(batch); err != nil {
					errChan <- err
				}
			}()
		} else {
			// wait again as batch was empty
		}
	}
}

func (p *Priority[Request, Response]) executeBatch(batch []*internalRequest[Request, Response]) error {
	var requestBatch []*KeyedRequest[Request]
	var links []trace.Link
	for _, i := range batch {
		requestBatch = append(requestBatch, &KeyedRequest[Request]{
			ID:      i.id,
			Request: i.request,
		})
		links = append(links, trace.Link{
			SpanContext: trace.SpanContextFromContext(i.ctx),
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), p.config.ExecutionTimeout)
	defer cancel()
	ctx, span := p.tracer.Start(
		ctx,
		"PQueue - executeBatch",
		trace.WithAttributes(
			attribute.String("queue.name", p.config.Name),
			attribute.Int("queue.batch_size", len(requestBatch)),
		),
		trace.WithLinks(links...),
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer span.End()
	responseChannel := make(chan *KeyedResponse[Response])
	go func() {
		err := p.executor.Execute(ctx, requestBatch, responseChannel)
		if err != nil {
			for _, req := range batch {
				req.response(nil, fmt.Errorf("failed to execute batch: %w", err))
			}
		}
	}()

	for x := 0; x < len(requestBatch); x++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case response := <-responseChannel:
			found := false
		l:
			for _, req := range batch {
				if response.ID != req.id {
					continue
				}
				req.response(response.Response, nil)
				found = true
				break l
			}
			if !found {
				return fmt.Errorf("responded to request that didnt exist: %s", response.ID)
			}
		}
	}
	close(responseChannel)

	return nil
}

func (p *Priority[Request, Response]) makeBatch(size int) []*internalRequest[Request, Response] {
	var batch []*internalRequest[Request, Response]
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
		case item := <-p.lowPriority:
			batch = append(batch, item)
		}

	}
}
