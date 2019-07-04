package sqsch

import (
	"context"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

// Create allocates the receive, delete, and errs channels and begins a polling loop for new messages
func Create(
	ctx context.Context,
	sqsapi sqsiface.SQSAPI,
	input *sqs.ReceiveMessageInput,
) (
	receive <-chan *sqs.Message,
	delete chan<- *sqs.Message,
	errs <-chan error,
) {
	receives := make(chan *sqs.Message)
	deletes := make(chan *sqs.Message)
	errors := make(chan error)

	go receiveLoop(ctx, sqsapi, input, receives, errors)
	go deleteLoop(ctx, sqsapi, *(input.QueueUrl), deletes, errors)
	go func() {
		<-ctx.Done()
		close(errors)
	}()

	return receives, deletes, errors
}

func receiveLoop(
	ctx context.Context,
	sqsapi sqsiface.SQSAPI,
	input *sqs.ReceiveMessageInput,
	receives chan<- *sqs.Message,
	errors chan<- error,
) {
	for {
		select {
		case <-ctx.Done():
			close(receives)
			return
		default:
			output, err := sqsapi.ReceiveMessageWithContext(ctx, input)

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

func deleteLoop(
	ctx context.Context,
	sqsapi sqsiface.SQSAPI,
	url string,
	deletes chan *sqs.Message,
	errors chan<- error,
) {
	for {
		select {
		case message := <-deletes:
			_, err := sqsapi.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      &url,
				ReceiptHandle: message.ReceiptHandle,
			})

			if err != nil {
				errors <- err
			}
		case <-ctx.Done():
			close(deletes)
			return
		}
	}
}
