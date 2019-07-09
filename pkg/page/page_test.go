package page

import (
	"errors"
	"context"
	"testing"

	"github.com/bendrucker/sqs-receive-channel/pkg/page/mocks"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestPager(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.TODO()
	reader := mocks.NewMockReader(ctrl)
	pager := New(reader)

	reader.
		EXPECT().
		MaxPageSize().
		Return(10).
		AnyTimes()

	reader.
		EXPECT().
		Read(ctx, 1).
		Return([]interface{}{"hello world"}, nil)

	go pager.Read(ctx, 1)
	result := <-pager.Results()
	assert.Equal(t, "hello world", result.(string))
}

func TestPagerErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.TODO()
	reader := mocks.NewMockReader(ctrl)
	pager := New(reader)

	reader.
		EXPECT().
		MaxPageSize().
		Return(10).
		AnyTimes()

	reader.
		EXPECT().
		Read(ctx, 1).
		Return(nil, errors.New("oops"))

	go pager.Read(ctx, 1)
	err := <-pager.Errors()
	assert.EqualError(t, err, "oops")
}

func TestPagerPages(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cases := []struct {
		max      int
		count    int
		expected []int
	}{
		{10, 20, []int{10, 10}},
		{10, 25, []int{10, 10, 5}},
		{10, 5, []int{5}},
	}

	for _, c := range cases {
		reader := mocks.NewMockReader(ctrl)
		pager := New(reader)

		reader.
			EXPECT().
			MaxPageSize().
			Return(c.max).
			AnyTimes()

		assert.Equal(t, c.expected, pager.Pages(c.count))
	}
}
