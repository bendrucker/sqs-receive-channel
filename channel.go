package sqsch

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

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
	receive <-chan *sqs.Message,
	delete chan<- *sqs.Message,
	errs <-chan error,
) {
	if options.ReceiveBufferSize == 0 {
		options.ReceiveBufferSize = 1
	}

	dispatch := Dispatch{
		Options: options,
	}

	receives := make(chan *sqs.Message, options.ReceiveBufferSize)
	deletes := make(chan *sqs.Message, MaxBatchSize)
	errors := make(chan error)

	go dispatch.Receive(ctx, receives, errors)
	go dispatch.Delete(ctx, deletes, errors)

	return receives, deletes, errors
}

// Receive runs a loop that receives messages from SQS until the supplied context is canceled.
// It checks for available space on the receive channel's buffer.
// It fetches up to that number of messages from SQS and sends them to the receive channel.
// If the receive buffer is full, it continues looping until capacity is detected.
// Because SQS bills per API request, ReceiveMessageInput.WaitTimeSeconds allows the loop to block
// for up to 20 seconds if no messages are available to receive which results in ~3 requests per minute
// instead of hundreds when your queue is idle.
func (d *Dispatch) Receive(ctx context.Context, receives chan<- *sqs.Message, errors chan<- error) {
	requests := make(chan ReceiveRequest)

	for i := 0; i < d.numGoroutine(); i++ {
		d.createReceiver(ctx, requests, receives, errors)
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			capacity := cap(receives) - len(receives)

			if capacity == 0 {
				continue
			}

			output, err := d.receiveMessages(ctx, capacity)

			if err != nil {
				errors <- err
				continue
			}

			for _, message := range output.Messages {
				receives <- message
			}
		}
	}
}

func (d *Dispatch) createReceiver(ctx context.Context, requests <-chan ReceiveRequest, receives chan<- *sqs.Message, errors chan<- error) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case request := <-requests:
				output, err := d.receiveMessages(ctx, request.Count)

				if err != nil {
					errors <- err
					continue
				}

				for _, message := range output.Messages {
					receives <- message
				}
			}
		}
	}()
}

func (d *Dispatch) receiveMessages(ctx context.Context, count int) (*sqs.ReceiveMessageOutput, error) {
	return d.SQS.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
		MaxNumberOfMessages: aws.Int64(int64(count)),
		WaitTimeSeconds:     aws.Int64(int64(MaxLongPollDuration.Seconds())),

		AttributeNames:        d.ReceiveMessageInput.AttributeNames,
		MessageAttributeNames: d.ReceiveMessageInput.MessageAttributeNames,
		QueueUrl:              d.ReceiveMessageInput.QueueUrl,
		VisibilityTimeout:     d.ReceiveMessageInput.VisibilityTimeout,
	})
}

func (d *Dispatch) numGoroutine() int {
	return int(math.Ceil(float64(d.ReceiveBufferSize) / MaxBatchSize))
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
