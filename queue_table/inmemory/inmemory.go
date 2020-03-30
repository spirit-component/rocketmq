package inmemory

import (
	"fmt"
	"sync/atomic"

	rmq "github.com/apache/rocketmq-client-go/core"
	"github.com/gogap/config"
	"github.com/sirupsen/logrus"
	"github.com/spirit-component/rocketmq/queue_table"
)

type InMemoryQueueTable struct {
	pullConsumer rmq.PullConsumer
	consumerConf *rmq.PullConsumerConfig

	topic          string
	expression     string
	queueTableConf config.Configuration

	queues       []rmq.MessageQueue
	queueOffsets map[int]*int64
	queueIDs     map[int]bool
}

func init() {
	queue_table.RegisterQueueTable("in-memory", NewInMemoryQueueTable)
}

func NewInMemoryQueueTable(consumer rmq.PullConsumer, topic, expr string, consumerConf *rmq.PullConsumerConfig, queueTableConf config.Configuration) (table queue_table.QueueTable, err error) {

	queueIDs := queueTableConf.GetInt32List("queue-ids")

	if len(queueIDs) == 0 {
		err = fmt.Errorf("provider[%s].queue-ids list is empty", "in-memory")
		return
	}

	mapQueueIDs := map[int]bool{}
	for _, id := range queueIDs {
		mapQueueIDs[int(id)] = true
	}

	return &InMemoryQueueTable{
		pullConsumer:   consumer,
		consumerConf:   consumerConf,
		topic:          topic,
		expression:     expr,
		queueTableConf: queueTableConf,
		queueIDs:       mapQueueIDs,
		queueOffsets:   make(map[int]*int64),
	}, nil
}

func (p *InMemoryQueueTable) Start() (err error) {

	queues := p.pullConsumer.FetchSubscriptionMessageQueues(p.topic)

	logrus.WithFields(
		logrus.Fields{
			"topic":       p.topic,
			"queue-count": len(queues),
			"expression":  p.expression,
			"provider":    "in-memory",
		},
	).Debugln("queues fetched")

	for i := 0; i < len(queues); i++ {
		if p.queueIDs[queues[i].ID] {
			p.queueOffsets[queues[i].ID] = new(int64)
			p.initQueueOffset(queues[i])
			p.queues = append(p.queues, queues[i])
		}
	}

	return
}

func (p *InMemoryQueueTable) Stop() (err error) {
	return
}

func (p *InMemoryQueueTable) Queues() []rmq.MessageQueue {
	return p.queues
}

func (p *InMemoryQueueTable) CurrentOffset(queueID int) (ret int64, err error) {
	return atomic.LoadInt64(p.queueOffsets[queueID]), nil
}

func (p *InMemoryQueueTable) UpdateOffset(queueID int, nextBeginOffset int64) error {
	atomic.StoreInt64(p.queueOffsets[queueID], nextBeginOffset)
	return nil
}

func (p *InMemoryQueueTable) initQueueOffset(mq rmq.MessageQueue) {
	pullResult := p.pullConsumer.Pull(mq, p.expression, 0, 1)
	atomic.StoreInt64(p.queueOffsets[mq.ID], pullResult.MaxOffset)

	logrus.WithFields(
		logrus.Fields{
			"topic":       p.topic,
			"queue-id":    mq.ID,
			"expression":  p.expression,
			"min-offset":  pullResult.MinOffset,
			"max-offset":  pullResult.MaxOffset,
			"next-offset": pullResult.NextBeginOffset,
			"status":      pullResult.Status,
		},
	).Debugln("init queue offset")
}
