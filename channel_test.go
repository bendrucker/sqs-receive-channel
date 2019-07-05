package sqsch

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/bendrucker/sqs-receive-channel/mock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func setup(t *testing.T) (context.Context, *mock.MockSQSAPI, func()) {
	ctrl := gomock.NewController(t)
	sqsapi := mock.NewMockSQSAPI(ctrl)
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(1)*time.Second)
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
		ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            input.QueueUrl,
			WaitTimeSeconds:     aws.Int64(20),
			MaxNumberOfMessages: aws.Int64(1),
		}).
		Return(&sqs.ReceiveMessageOutput{
			Messages: []*sqs.Message{&sqs.Message{
				Body: aws.String("hello world"),
			}},
		}, nil).
		AnyTimes()

	receive, _, _ := Start(ctx, Options{
		SQS:                 sqsapi,
		ReceiveMessageInput: input,
	})

	message := <-receive
	assert.Equal(t, "hello world", aws.StringValue(message.Body))
}

func TestDelete(t *testing.T) {
	ctx, sqsapi, finish := setup(t)
	defer finish()

	ctx, cancel := context.WithCancel(ctx)

	input := &sqs.ReceiveMessageInput{
		QueueUrl: aws.String("http://foo.bar"),
	}

	message := &sqs.Message{
		Body:          aws.String("hello world"),
		ReceiptHandle: aws.String("handle"),
	}

	sqsapi.
		EXPECT().
		ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            input.QueueUrl,
			WaitTimeSeconds:     aws.Int64(20),
			MaxNumberOfMessages: aws.Int64(1),
		}).
		Return(&sqs.ReceiveMessageOutput{
			Messages: []*sqs.Message{},
		}, nil).
		AnyTimes()

	sqsapi.
		EXPECT().
		DeleteMessageBatchWithContext(ctx, &sqs.DeleteMessageBatchInput{
			QueueUrl: aws.String("http://foo.bar"),
			Entries: []*sqs.DeleteMessageBatchRequestEntry{
				&sqs.DeleteMessageBatchRequestEntry{
					Id:            aws.String("0"),
					ReceiptHandle: aws.String("handle"),
				},
			},
		}).
		Return(&sqs.DeleteMessageBatchOutput{
			Failed: []*sqs.BatchResultErrorEntry{},
			Successful: []*sqs.DeleteMessageBatchResultEntry{
				&sqs.DeleteMessageBatchResultEntry{
					Id: aws.String("0"),
				},
			},
		}, nil).
		Do(func(_ interface{}, _ interface{}) {
			cancel()
		})

	_, delete, _ := Start(ctx, Options{
		SQS:                 sqsapi,
		ReceiveMessageInput: input,
	})

	delete <- message
	<-ctx.Done()
}

func TestReceiveError(t *testing.T) {
	ctx, sqsapi, finish := setup(t)
	defer finish()

	input := &sqs.ReceiveMessageInput{
		QueueUrl: aws.String("http://foo.bar"),
	}

	sqsapi.
		EXPECT().
		ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            input.QueueUrl,
			WaitTimeSeconds:     aws.Int64(20),
			MaxNumberOfMessages: aws.Int64(1),
		}).
		Return(nil, errors.New("SQS error")).
		AnyTimes()

	_, _, errs := Start(ctx, Options{
		SQS:                 sqsapi,
		ReceiveMessageInput: input,
	})

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
		ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            input.QueueUrl,
			WaitTimeSeconds:     aws.Int64(20),
			MaxNumberOfMessages: aws.Int64(1),
		}).
		Return(&sqs.ReceiveMessageOutput{
			Messages: []*sqs.Message{},
		}, nil).
		AnyTimes()

	sqsapi.
		EXPECT().
		DeleteMessageBatchWithContext(ctx, &sqs.DeleteMessageBatchInput{
			QueueUrl: aws.String("http://foo.bar"),
			Entries: []*sqs.DeleteMessageBatchRequestEntry{
				&sqs.DeleteMessageBatchRequestEntry{
					Id:            aws.String("0"),
					ReceiptHandle: aws.String("handle"),
				},
			},
		}).
		Return(nil, errors.New("SQS error"))

	_, deletes, errs := Start(ctx, Options{
		SQS:                 sqsapi,
		ReceiveMessageInput: input,
	})

	deletes <- message
	err := <-errs
	assert.EqualError(t, err, "SQS error")
}
