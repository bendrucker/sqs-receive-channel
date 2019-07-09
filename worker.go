package sqsw

import (
	"github.com/bendrucker/sqs-receive-channel/pkg/receive"
	"github.com/bendrucker/werk"
)

// QueueWorker represents an SQS worker application that processes messages on a
// single queue. It maintains sub-routines for receiving and deleting messages
// depending on user-specified concurrency and available messages.

type QueueWorker struct {
	handler  Handler
	timeout  time.Duration
	receiver *receive.Receive
	handlers *werk.Pool
	deletes  chan werk.Work
	deleters *werk.Pool
}

type Handler func(*sqs.Message) error
type TimeoutFunc(time.Duration) time.Duration

func (w *QueueWorker) Start() {
	w.receiver.Start()
	for message := range w.receiver.Results() {
		w.handlers.Do(Work{message, w.timeout}, w.handler)
		w.deletes <- message
	}
}
