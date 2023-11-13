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

type PoolConfig struct {
	BaseWorkers      int
	Name             string
	ExecutionTimeout time.Duration
}

type Pool[Request any, Response any] struct {
	executor Executor[Request, Response]
	config   PoolConfig
	pool     semaphore.Semaphore
	tracer   trace.Tracer
}

func NewPool[Request any, Response any](config PoolConfig, executor Executor[Request, Response], tracer trace.Tracer) *Pool[Request, Response] {
	return &Pool[Request, Response]{
		config:   config,
		executor: executor,
		tracer:   tracer,
		pool:     semaphore.New(config.BaseWorkers),
	}
}

func (p *Pool[Request, Response]) Execute(ctx context.Context, items []*Request) ([]*Response, error) {
	var internalRequests []*internalRequest[Request, Response]

	responsesChan := make(chan *Response, len(items))
	errorsChan := make(chan error, len(items))
	wg := sync.WaitGroup{}
	wg.Add(len(items))
	for _, i := range items {
		id := uuid.New()
		internalRequests = append(internalRequests, &internalRequest[Request, Response]{
			request: i,
			id:      id.String(),
			ctx:     ctx,
			response: func(response *Response, err error) {
				if response != nil {
					responsesChan <- response
				}
				if err != nil {
					errorsChan <- err
				}
				wg.Done()
			},
		})
	}
	wg.Wait()
	var responses []*Response
	for x := 0; x < len(items); x++ {
		select {
		case err := <-errorsChan:
			return nil, err
		case response := <-responsesChan:
			responses = append(responses, response)
		}
	}
	return responses, nil
}

func (p *Pool[Request, Response]) executeBatch(batch []*internalRequest[Request, Response]) error {
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
	if err := p.pool.Acquire(context.Background(), 1); err != nil {
		return err
	}
	defer func() {
		p.pool.Release(1)
	}()
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
