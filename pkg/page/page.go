package page

import (
	"context"
	"math"
	"sync"
)

// Reader is the interface implemented by objects that can read a page of results
type Reader interface {
	Read(ctx context.Context, count int) ([]interface{}, error)
	MaxPageSize() int
}

// New returns a new Pager given the Reader and
func New(r Reader) *Pager {
	return &Pager{
		reader:  r,
		results: make(chan interface{}),
		errors:  make(chan error),
	}
}

// Pager fulfills requests for an arbitrary number of records across pages with a fixed maximum length. It writes results and errors to channels so that results are available immediately upon arrival even if other pages are still pending.
type Pager struct {
	reader Reader

	results chan interface{}
	errors  chan error
}

func (p *Pager) Read(ctx context.Context, count int) {
	wg := &sync.WaitGroup{}

	for _, size := range p.Pages(count) {
		wg.Add(1)
		size := size
		go func() {
			p.ReadPage(ctx, size)
			wg.Done()
		}()
	}

	wg.Wait()
}

func (p *Pager) ReadPage(ctx context.Context, size int) {
	if results, err := p.reader.Read(ctx, size); err != nil {
		p.errors <- err
	} else {
		for _, result := range results {
			p.results <- result
		}
	}
}

// Results returns a read-only copy of the results channel
func (p *Pager) Results() <-chan interface{} {
	return p.results
}

// Errors returns a read-only copy of the errors channel
func (p *Pager) Errors() <-chan error {
	return p.errors
}

// Pages creates a slice of page sizes based on the supplied count and pageSize
// Example: count=25, pageSize=10
// Result: [10, 10, 5]
func (p *Pager) Pages(count int) []int {
	pages := make([]int, int(math.Ceil(float64(count)/float64(p.reader.MaxPageSize()))))

	for i := range pages {
		c := int(math.Min(float64(count), float64(p.reader.MaxPageSize())))
		count = count - c

		pages[i] = c
	}

	return pages
}
