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
	Options Options

	receives chan *sqs.Message
	deletes  chan *sqs.Message
	errors   chan error
}

// Options represents the user-configurable options for a Dispatch
type Options struct {
	Receive ReceiveOptions
	Delete  DeleteOptions

	SQS sqsiface.SQSAPI
}

// Defaults sets default values
func (o *Options) Defaults() {
	o.Receive.Defaults()
	o.Delete.Defaults()
}

// ReceiveOptions configures receiving of messages from SQS
type ReceiveOptions struct {
	BufferSize          int
	RecieveMessageInput *sqs.ReceiveMessageInput
}

// Defaults sets default values
func (ro *ReceiveOptions) Defaults() {
	if ro.BufferSize == 0 {
		ro.BufferSize = 1
	}
}

// DeleteOptions configures deletion of messages from SQS
type DeleteOptions struct {
	Interval    time.Duration
	Concurrency int
}

// Defaults sets default values
func (do *DeleteOptions) Defaults() {
	if do.Interval == 0 {
		do.Interval = time.Duration(1) * time.Second
	}

	if do.Concurrency == 0 {
		do.Concurrency = 1
	}
}

// Start allocates channels, begins receiving, and begins processing deletes
func Start(ctx context.Context, options Options) (
	<-chan *sqs.Message,
	chan<- *sqs.Message,
	<-chan error,
) {
	options.Defaults()
	dispatch := Dispatch{
		Options:  options,
		receives: make(chan *sqs.Message, options.Receive.BufferSize),
		deletes:  make(chan *sqs.Message, MaxBatchSize),
		errors:   make(chan error),
	}

	dispatch.Receive(ctx)
	dispatch.Delete(ctx)

	return dispatch.receives, dispatch.deletes, dispatch.errors
}

// QueueURL returns the SQS Queue URL specified with Options.Receive.ReceiveMessageInput
func (d *Dispatch) QueueURL() *string {
	return d.Options.Receive.RecieveMessageInput.QueueUrl
}

// ReceiveCapacity returns the available space in the receive channel's buffer.
// This is used to determine how many ReceiveMessage requests to issue and how
// many messages (count) are requested in each.
func (d *Dispatch) ReceiveCapacity() int {
	// TODO: This should be user specifiable and based on a different channel buffer
	// An implementer will have workers:
	// func worker(workers chan<- Worker, work <-chan Work) {
	// 		workers <- Worker{work}
	// 		doWork(<-work)
	// }
	//
	// workers should be a buffered channel with cap set to the desired concurrency
	// CountFunc should return len(workers)—the number of workers that have
	// have not been assigned by the dispatcher by reading them from the channel
	// and writing to their work channel
	//
	// Using the size of the receive buffer, the application will fetch eagerly.
	// This is ok for lower throughput applications and inexpensive tasks where
	// the visibility timeout is ~10x the expected time to processing.
	//
	// But given a 30s CPU-intensive job w/ a 60s timeout, the application would
	// start buffering messages for ~30s before even starting work on them, resulting
	// in lots of timeouts.
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
	input := d.Options.Receive.RecieveMessageInput
	output, err := d.Options.SQS.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
		MaxNumberOfMessages: aws.Int64(int64(count)),
		WaitTimeSeconds:     aws.Int64(int64(MaxLongPollDuration.Seconds())),
		QueueUrl:            d.QueueURL(),

		AttributeNames:        input.AttributeNames,
		MessageAttributeNames: input.MessageAttributeNames,
		VisibilityTimeout:     input.VisibilityTimeout,
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
func (d *Dispatch) Delete(ctx context.Context) {
	batches := d.BatchDeletes(d.deletes)

	for i := 0; i < d.Options.Delete.Concurrency; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case entries := <-batches:
					output, err := d.Options.SQS.DeleteMessageBatchWithContext(ctx, &sqs.DeleteMessageBatchInput{
						Entries:  entries,
						QueueUrl: d.QueueURL(),
					})

					if err != nil {
						d.errors <- err
						continue
					}

					for i, failure := range output.Failed {
						d.errors <- &BatchDeleteError{
							Code:          *failure.Code,
							Message:       *failure.Message,
							ReceiptHandle: *entries[i].ReceiptHandle,
						}
					}
				}
			}
		}()
	}
}

// BatchDeletes buffers messages received on the delete channel,
// batching according to the Delete.Interval and the MaxBatchSize
func (d *Dispatch) BatchDeletes(deletes <-chan *sqs.Message) <-chan []*sqs.DeleteMessageBatchRequestEntry {
	input := make(chan interface{})
	go func() {
		for m := range deletes {
			input <- m
		}
	}()

	batches := bach.NewBatch(input, MaxBatchSize, d.Options.Delete.Interval)
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
