# sqs-receive-channel [![Build Status](https://travis-ci.org/bendrucker/sqs-receive-channel.svg?branch=master)](https://travis-ci.org/bendrucker/sqs-receive-channel) [![GoDoc](https://godoc.org/github.com/bendrucker/sqs-receive-channel?status.svg)](https://godoc.org/github.com/bendrucker/sqs-receive-channel)

This package (`package sqsch`) provides a channel-oriented interface for processing messages from an SQS queue. 

SQS charges per API request. This package implements [Action Batching](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-throughput-horizontal-scaling-and-batching.html#request-batching) as recommended by the SQS docs to make efficient use of API calls. 

* Fetches only as many messages as the receive channel can buffer `ReceiveBufferSize - len(receive)`) in batches
* Uses [SQS long polling](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html) to reduce requests when no messages are available
* Deletes messages in batches, flushing when `MaxBatchSize` (10) or `DeleteInterval` is reached.

## Usage

```go
import (
  "context"
  "time"

  "github.com/bendrucker/sqs-receive-channel"
)

func main() {
  receive, delete, errs := sqsch.Start(context.TODO(), sqsch.Options{
    ReceiveBufferSize: 10,
    DeleteInterval: time.Duration(1) * time.Second
  })

  go func() {
    message := <-receive
    fmt.Println(message.Body)

		// SQS DeleteMessageBatch API will be called after:
		// a) 1 second or b) 9 more messages are processed
    delete <- message
  }()

  go func() {
    err := <-errs
    fmt.Println(err)
  }()
}
```

## Example

The following example illustrates the API calls made by this package in a "bursty" application. This scenario envisions a queue that is mostly idle, but receives large numbers of messages on occasion.

* Configuration: `ReceiveBufferSize=10, DeleteInterval=time.Duration(1)*time.Second`,
* Queue is idle for 55 seconds
* 10 messages become available
* All messages are read from the receive channel immediately
* All messages are handled and sent to the delete channel within 100ms

The following API requests will be performed in the first 60s:

* 4 calls to `ReceiveMessage` (2 that return empty, 1 that returns 10 messages, and then 1 that remains open at 1:00)
* 1 call to `DeleteMessageBatch`, containing 10 message handles

Compared to a naive approach (1 request per message, short polling), this can reduce requests to the SQS API by a considerable margin (1-3 orders of magnitude). Results will vary depending on throughput. For additional concurrency considerations, see [future work](#future-work).

## Future Work

* Allow `ReceiveBufferSize > 10` and issue parallel `ReceiveMessage` requests where applicable
* Process batch delete requests in parallel

## See Also

* [AWS SDK for Java: `AmazonSQSBufferedAsyncClient`](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-client-side-buffering-request-batching.html)
