package queue_table

import (
	"fmt"

	rmq "github.com/apache/rocketmq-client-go"
	"github.com/apache/rocketmq-client-go/primitive"
	"github.com/gogap/config"
)

type NewQueueTableFunc func(consumer rmq.PullConsumer, topic, expr, instanceName string, queueTableConf config.Configuration) (QueueTable, error)

var (
	queueTables = make(map[string]NewQueueTableFunc)
)

type QueueTable interface {
	Start() error
	Stop() error

	Queues() []primitive.MessageQueue
}

func RegisterQueueTable(driverName string, fn NewQueueTableFunc) (err error) {

	if len(driverName) == 0 {
		err = fmt.Errorf("queue table driver name is empty")
		return
	}

	if fn == nil {
		err = fmt.Errorf("driver of %s's NewQueueTableFunc is nil")
		return
	}

	if _, exist := queueTables[driverName]; exist {
		err = fmt.Errorf("queue table of: %d, already registered")
		return
	}

	queueTables[driverName] = fn

	return
}

func NewQueueTable(driverName string, consumer rmq.PullConsumer, topic, expr, instanceName string, queueTableConf config.Configuration) (table QueueTable, err error) {

	if len(driverName) == 0 {
		err = fmt.Errorf("queue table driver name is empty")
		return
	}

	if consumer == nil {
		err = fmt.Errorf("pull consumer is nil")
		return
	}

	fn, exist := queueTables[driverName]
	if !exist {
		err = fmt.Errorf("queue table of: %d, not registered")
		return
	}

	return fn(consumer, topic, expr, instanceName, queueTableConf)
}
