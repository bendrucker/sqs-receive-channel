package receive

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReceive(t *testing.T) {
	ctx := context.TODO()
	r := New(Options{
		MaxCount: 1,
		DoFunc: func(count Request) ([]interface{}, error) {
			assert.Equal(t, 1, int(count))
			return []interface{}{"hello world"}, nil
		},
		CountFunc: func() int {
			return 1
		},
	})

	r.Start(ctx)
	result := <-r.Results()

	assert.Equal(t, "hello world", result.(string))
}

func TestReceiveErrors(t *testing.T) {
	ctx := context.TODO()
	r := New(Options{
		MaxCount: 1,
		DoFunc: func(count Request) ([]interface{}, error) {
			assert.Equal(t, 1, int(count))
			return nil, errors.New("oops")
		},
		CountFunc: func() int {
			return 1
		},
	})

	r.Start(ctx)
	err := <-r.Errors()

	assert.EqualError(t, err, "oops")
}

func TestReceiveRequests(t *testing.T) {
	cases := []struct {
		max      int
		count    int
		expected []Request
	}{
		{10, 20, []Request{Request(10), Request(10)}},
		{10, 25, []Request{Request(10), Request(10), Request(5)}},
		{10, 5, []Request{Request(5)}},
	}

	for _, c := range cases {
		r := New(Options{MaxCount: c.max})
		assert.Equal(t, c.expected, r.Requests(c.count))
	}
}

func TestReceiveDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	r := New(Options{
		MaxCount: 1,
		CountFunc: func() int {
			return 0
		},
	})

	r.Start(ctx)
	cancel()
	<-r.done
	assert.False(t, r.started)
}

func TestReceivePanicInvalid(t *testing.T) {
	assert.Panics(t, func() {
		New(Options{MaxCount: 0})
	})
}

func TestReceivePanicAlreadyStarted(t *testing.T) {
	ctx := context.TODO()
	r := New(Options{
		MaxCount: 1,
		CountFunc: func() int {
			return 0
		},
	})

	r.Start(ctx)
	assert.PanicsWithValue(t, "receive already started", func() {
		r.Start(ctx)
	})
}
