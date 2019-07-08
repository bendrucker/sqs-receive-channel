package receive

import (
	"context"
	"math"
	"sync"
)

// Receive manages a loop that manages calls to a function (DoFunc)
// and outputs results to channels (results and errors).
// It manages changing concurrency over time by executing CountFunc
// and splitting that count over calls to DoFunc with count <= MaxCount
type Receive struct {
	Options

	results chan interface{}
	errors  chan error
	done    chan bool

	started bool
}

// Options represents the configurable parameters for Receive
type Options struct {
	MaxCount  int
	CountFunc CountFunc
	DoFunc    DoFunc
}

// CountFunc returns the number of results that can be received
type CountFunc func() int

// DoFunc takes a receive request and should return a result and error
type DoFunc func(Request) ([]interface{}, error)

// Request specifies the number of results to try to receive
type Request int

// New initiailizes a new Receive instance
func New(o Options) *Receive {
	if o.MaxCount <= 0 {
		panic("MaxCount must be > 0")
	}

	return &Receive{
		Options: o,
		results: make(chan interface{}),
		errors:  make(chan error),
		done:    make(chan bool, 1),
	}
}

// Start executes a new goroutine that executes a receive loop
func (r *Receive) Start(ctx context.Context) {
	if r.started {
		panic("receive already started")
	} else {
		r.started = true
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				r.started = false
				r.done <- true
				return
			default:
				r.Run()
			}
		}
	}()
}

// Run executes one run-through of the receive loop, executing the number of
// requests specified by CountFunc
func (r *Receive) Run() {
	wg := &sync.WaitGroup{}

	for _, request := range r.Requests(r.CountFunc()) {
		wg.Add(1)

		go func(req Request) {
			r.Do(req)
			wg.Done()
		}(request)
	}

	wg.Wait()
}

// Do executes a request, calling DoFunc and writing its result/error to the
// corresponding channels
func (r *Receive) Do(request Request) {
	if results, err := r.DoFunc(request); err != nil {
		r.errors <- err
	} else {
		for _, result := range results {
			r.results <- result
		}
	}
}

// Requests creates a slice of requests sized based on the supplied count and
// the configured MaxCount.
// Example: count=25, MaxCount=10
// Result: [Request(10), Request(10), Request(5)]
func (r *Receive) Requests(count int) []Request {
	requests := make([]Request, int(math.Ceil(float64(count)/float64(r.MaxCount))))

	for i := range requests {
		c := int(math.Min(float64(count), float64(r.MaxCount)))
		count = count - c

		requests[i] = Request(c)
	}

	return requests
}

// Results returns a read-only copy of the results channel
func (r *Receive) Results() <-chan interface{} {
	return r.results
}

// Errors returns a read-only copy of the errors channel
func (r *Receive) Errors() <-chan error {
	return r.errors
}
