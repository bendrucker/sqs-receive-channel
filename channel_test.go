package sqsch

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/bendrucker/sqs-receive-channel/mock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func setup(t *testing.T) (context.Context, *mock.MockSQSAPI, func()) {
	ctrl := gomock.NewController(t)
	sqsapi := mock.NewMockSQSAPI(ctrl)
	ctx := context.Background()
	return ctx, sqsapi, ctrl.Finish
}

func TestReceive(t *testing.T) {
	ctx, sqsapi, finish := setup(t)
	defer finish()

	input := &sqs.ReceiveMessageInput{
		QueueUrl: aws.String("http://foo.bar"),
	}

	sqsapi.
		EXPECT().
		ReceiveMessageWithContext(ctx, input).
		Return(&sqs.ReceiveMessageOutput{
			Messages: []*sqs.Message{&sqs.Message{
				Body: aws.String("hello world"),
			}},
		}, nil).
		AnyTimes()

	receive, _, _ := Create(ctx, sqsapi, input)

	message := <-receive
	assert.Equal(t, "hello world", aws.StringValue(message.Body))
}

func TestDelete(t *testing.T) {
	ctx, sqsapi, finish := setup(t)
	defer finish()

	input := &sqs.ReceiveMessageInput{
		QueueUrl: aws.String("http://foo.bar"),
	}

	message := &sqs.Message{
		Body:          aws.String("hello world"),
		ReceiptHandle: aws.String("handle"),
	}

	sqsapi.
		EXPECT().
		ReceiveMessageWithContext(ctx, input).
		Return(&sqs.ReceiveMessageOutput{
			Messages: []*sqs.Message{},
		}, nil).
		AnyTimes()

	sqsapi.
		EXPECT().
		DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
			QueueUrl:      aws.String("http://foo.bar"),
			ReceiptHandle: aws.String("handle"),
		}).
		Return(&sqs.DeleteMessageOutput{}, nil)

	_, delete, _ := Create(ctx, sqsapi, input)

	delete <- message
}

func TestReceiveError(t *testing.T) {
	ctx, sqsapi, finish := setup(t)
	defer finish()

	input := &sqs.ReceiveMessageInput{
		QueueUrl: aws.String("http://foo.bar"),
	}

	sqsapi.
		EXPECT().
		ReceiveMessageWithContext(ctx, input).
		Return(nil, errors.New("SQS error")).
		AnyTimes()

	_, _, errs := Create(ctx, sqsapi, input)

	err := <-errs
	assert.EqualError(t, err, "SQS error")
}

func TestDeleteError(t *testing.T) {
	ctx, sqsapi, finish := setup(t)
	defer finish()

	input := &sqs.ReceiveMessageInput{
		QueueUrl: aws.String("http://foo.bar"),
	}

	message := &sqs.Message{
		Body:          aws.String("hello world"),
		ReceiptHandle: aws.String("handle"),
	}

	sqsapi.
		EXPECT().
		ReceiveMessageWithContext(ctx, input).
		Return(&sqs.ReceiveMessageOutput{
			Messages: []*sqs.Message{},
		}, nil).
		AnyTimes()

	sqsapi.
		EXPECT().
		DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
			QueueUrl:      aws.String("http://foo.bar"),
			ReceiptHandle: aws.String("handle"),
		}).
		Return(nil, errors.New("SQS error"))

	_, deletes, errs := Create(ctx, sqsapi, input)

	deletes <- message
	err := <-errs
	assert.EqualError(t, err, "SQS error")
}
