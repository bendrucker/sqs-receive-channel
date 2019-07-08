package sqsch

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/bendrucker/sqs-receive-channel/pkg/receive"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"

	"github.com/bendrucker/bach"
)

const (
	// MaxLongPollDuration is the maximum duration for SQS long polling
	// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html
	MaxLongPollDuration = 20 * time.Second

	// MaxBatchSize is the largest number of messages that can be processed in a batch SQS request.
	// It applies to ReceiveMessage (MaxNumberOfMessages) and DeleteMessageBatch (DeleteMessageBatchRequestEntry.N)
	// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html
	// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessageBatch.html
	MaxBatchSize = 10
)

// Dispatch provides methods for processing messages SQS via channels
type Dispatch struct {
	Options

	receives chan *sqs.Message
	deletes  chan *sqs.Message
	errors   chan error
}

// Options represents the user-configurable options for a Dispatch
type Options struct {
	ReceiveBufferSize int
	DeleteInterval    time.Duration

	SQS                 sqsiface.SQSAPI
	ReceiveMessageInput *sqs.ReceiveMessageInput
}

// Start allocates channels, begins receiving, and begins processing deletes
func Start(ctx context.Context, options Options) (
	<-chan *sqs.Message,
	chan<- *sqs.Message,
	<-chan error,
) {
	if options.ReceiveBufferSize == 0 {
		options.ReceiveBufferSize = 1
	}

	dispatch := Dispatch{
		Options:  options,
		receives: make(chan *sqs.Message, options.ReceiveBufferSize),
		deletes:  make(chan *sqs.Message, MaxBatchSize),
		errors:   make(chan error),
	}

	dispatch.Receive(ctx)
	go dispatch.Delete(ctx, dispatch.deletes, dispatch.errors)

	return dispatch.receives, dispatch.deletes, dispatch.errors
}

func (d *Dispatch) ReceiveCapacity() int {
	return cap(d.receives) - len(d.receives)
}

// Receive runs a loop that receives messages from SQS until the supplied context is canceled.
// It checks for available space on the receive channel's buffer.
// It fetches up to that number of messages from SQS and sends them to the receive channel.
// If the receive buffer is full, it continues looping until capacity is detected.
// Because SQS bills per API request, ReceiveMessageInput.WaitTimeSeconds allows the loop to block
// for up to 20 seconds if no messages are available to receive which results in ~3 requests per minute
// instead of hundreds when your queue is idle.
func (d *Dispatch) Receive(ctx context.Context) {
	receive := receive.New(receive.Options{
		MaxCount:  MaxBatchSize,
		CountFunc: d.ReceiveCapacity,
		DoFunc: func(count receive.Request) ([]interface{}, error) {
			return d.doReceive(ctx, count)
		},
	})

	receive.Start(ctx)

	go func() {
		for message := range receive.Results() {
			d.receives <- message.(*sqs.Message)
		}
	}()

	go func() {
		for err := range receive.Errors() {
			d.errors <- err
		}
	}()
}

func (d *Dispatch) doReceive(ctx context.Context, count receive.Request) ([]interface{}, error) {
	messages, err := d.receiveMessages(ctx, int(count))

	if err != nil {
		return nil, err
	}

	return wrapMessages(messages), nil
}

func wrapMessages(messages []*sqs.Message) []interface{} {
	results := make([]interface{}, len(messages))

	for i, message := range messages {
		results[i] = message
	}

	return results
}

func (d *Dispatch) receiveMessages(ctx context.Context, count int) ([]*sqs.Message, error) {
	output, err := d.SQS.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
		MaxNumberOfMessages: aws.Int64(int64(count)),
		WaitTimeSeconds:     aws.Int64(int64(MaxLongPollDuration.Seconds())),

		AttributeNames:        d.ReceiveMessageInput.AttributeNames,
		MessageAttributeNames: d.ReceiveMessageInput.MessageAttributeNames,
		QueueUrl:              d.ReceiveMessageInput.QueueUrl,
		VisibilityTimeout:     d.ReceiveMessageInput.VisibilityTimeout,
	})

	if err != nil {
		return nil, err
	}

	return output.Messages, nil
}

// BatchDeleteError represents an error returned from SQS in response to a DeleteMessageBatch request
type BatchDeleteError struct {
	Code          string
	Message       string
	ReceiptHandle string
}

func (err *BatchDeleteError) Error() string {
	return fmt.Sprintf("SQS batch delete error: %s (%s)", err.Message, err.Code)
}

// Delete processes messages received on the delete channel until the supplied context is canceled.
// It batching messages with BatchDeletes and calls the SQS DeleteMessageBatch API to trigger deletion.
// If there are failures in the DeleteMessageBatchOutput, it sends one error per failure to the errors channel.
func (d *Dispatch) Delete(ctx context.Context, deletes <-chan *sqs.Message, errors chan<- error) {
	batches := d.BatchDeletes(deletes)

	for {
		select {
		case <-ctx.Done():
			return
		case entries := <-batches:
			output, err := d.SQS.DeleteMessageBatchWithContext(ctx, &sqs.DeleteMessageBatchInput{
				Entries:  entries,
				QueueUrl: d.ReceiveMessageInput.QueueUrl,
			})

			if err != nil {
				errors <- err
				continue
			}

			for i, failure := range output.Failed {
				errors <- &BatchDeleteError{
					Code:          *failure.Code,
					Message:       *failure.Message,
					ReceiptHandle: *entries[i].ReceiptHandle,
				}
			}
		}
	}
}

// BatchDeletes buffers messages received on the delete channel,
// batching according to the DeleteInterval and the MaxBatchSize
func (d *Dispatch) BatchDeletes(deletes <-chan *sqs.Message) <-chan []*sqs.DeleteMessageBatchRequestEntry {
	input := make(chan interface{})
	go func() {
		for m := range deletes {
			input <- m
		}
	}()

	batches := bach.NewBatch(input, MaxBatchSize, d.DeleteInterval)
	output := make(chan []*sqs.DeleteMessageBatchRequestEntry)

	go func() {
		for batch := range batches {
			entries := make([]*sqs.DeleteMessageBatchRequestEntry, len(batch))

			for i, message := range batch {
				entries[i] = &sqs.DeleteMessageBatchRequestEntry{
					Id:            aws.String(strconv.Itoa(i)),
					ReceiptHandle: message.(*sqs.Message).ReceiptHandle,
				}
			}

			output <- entries
		}
	}()

	return output
}
