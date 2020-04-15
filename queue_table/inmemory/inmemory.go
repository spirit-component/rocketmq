package inmemory

import (
	"fmt"
	"sync/atomic"

	rmq "github.com/apache/rocketmq-client-go"
	"github.com/gogap/config"
	"github.com/sirupsen/logrus"
	"github.com/spirit-component/rocketmq/queue_table"
)

type InMemoryQueueTable struct {
	pullConsumer rmq.PullConsumer
	consumerConf *rmq.PullConsumerConfig

	topic          string
	expression     string
	instanceName   string
	queueTableConf config.Configuration

	queues   []rmq.MessageQueue
	queueIDs map[int]bool
}

func init() {
	queue_table.RegisterQueueTable("in-memory", NewInMemoryQueueTable)
}

func NewInMemoryQueueTable(consumer rmq.PullConsumer, topic, expr, instanceName string, queueTableConf config.Configuration) (table queue_table.QueueTable, err error) {

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
		instanceName:   instanceName,
		topic:          topic,
		expression:     expr,
		queueTableConf: queueTableConf,
		queueIDs:       mapQueueIDs,
		queueOffsets:   make(map[string]*int64),
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
			key := fmt.Sprintf("%s:%d", queues[i].Broker, queues[i].ID)
			p.queueOffsets[key] = new(int64)
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
