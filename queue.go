package queue

import (
	"context"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"sync"
	"time"
)

type PriorityConfig struct {
	PoolConfig
	BatchSize    int
	BufferSize   int
	WaitDuration time.Duration
}

type Level string

var (
	High Level = "high"
	Low  Level = "low"
)

type Logger interface {
	Error(msg string, args ...any)
}

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
	config       PriorityConfig
	pool         *Pool[Request, Response]
	tracer       trace.Tracer
	logger       Logger
}

func NewPriority[Request any, Response any](config PriorityConfig, executor Executor[Request, Response], tracer trace.Tracer, logger Logger) *Priority[Request, Response] {
	p := &Priority[Request, Response]{
		highPriority: make(chan *internalRequest[Request, Response], config.BufferSize),
		lowPriority:  make(chan *internalRequest[Request, Response], config.BufferSize),
		config:       config,
		tracer:       tracer,
		pool:         NewPool[Request, Response](config.PoolConfig, executor, tracer),
		logger:       logger,
	}

	go func() {
		if err := p.startWorkerPool(); err != nil {
			logger.Error("worker pool failed to start", err)
		}
	}()
	return p
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
	wg.Add(1)
	switch level {
	case High:
		p.highPriority <- request
	default:
		p.lowPriority <- request
	}

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
				if err := p.pool.executeBatch(batch); err != nil {
					for _, i := range batch {
						i.response(nil, err)
					}
				}
			}()
		} else {
			// wait again as batch was empty
		}
	}
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
