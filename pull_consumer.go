package rocketmq

import (
	"fmt"
	"sync/atomic"
	"time"

	rmq "github.com/apache/rocketmq-client-go/core"
	"github.com/gogap/config"
	"github.com/sirupsen/logrus"
)

type PullConsumer struct {
	consumerConfig *rmq.PullConsumerConfig
	pullConsumer   rmq.PullConsumer
	consumeFunc    ConsumeFunc

	topic      string
	expression string
	maxFetch   int

	queues       []rmq.MessageQueue
	queueOffsets map[int]*int64

	queueIDs map[int]bool

	stopSignal chan struct{}
}

func (p *PullConsumer) Start() (err error) {
	err = p.pullConsumer.Start()
	if err != nil {
		return
	}

	queues := p.pullConsumer.FetchSubscriptionMessageQueues(p.topic)

	// TODO: add more complex queue distributor, add queues count monitor and replancer
	logrus.WithFields(
		logrus.Fields{
			"topic":       p.topic,
			"queue-count": len(queues),
			"expression":  p.expression,
		},
	).Debugln("Queues fetched")

	for i := 0; i < len(queues); i++ {
		if p.queueIDs[queues[i].ID] {
			p.queueOffsets[queues[i].ID] = new(int64)
			p.initQueueOffset(queues[i])
			p.queues = append(p.queues, queues[i])
		}
	}

	p.stopSignal = make(chan struct{})

	go p.pull()

	return
}
func (p *PullConsumer) initQueueOffset(mq rmq.MessageQueue) {
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

func (p *PullConsumer) pull() {

	for {
		for _, mq := range p.queues {
			pullResult := p.pullConsumer.Pull(mq, p.expression, atomic.LoadInt64(p.queueOffsets[mq.ID]), p.maxFetch)

			switch pullResult.Status {
			case rmq.PullNoNewMsg:
			case rmq.PullFound:
				{

					atomic.StoreInt64(p.queueOffsets[mq.ID], pullResult.NextBeginOffset)

					for i := 0; i < len(pullResult.Messages); i++ {
						err := p.consumeFunc(pullResult.Messages[i])
						if err != nil {
							atomic.StoreInt64(p.queueOffsets[mq.ID], pullResult.Messages[i].QueueOffset)
							break
						}
					}
				}
			case rmq.PullNoMatchedMsg:
				atomic.StoreInt64(p.queueOffsets[mq.ID], pullResult.NextBeginOffset)
			case rmq.PullOffsetIllegal:
				atomic.StoreInt64(p.queueOffsets[mq.ID], pullResult.NextBeginOffset)
			case rmq.PullBrokerTimeout:
				logrus.WithFields(
					logrus.Fields{
						"topic":       p.topic,
						"expression":  p.expression,
						"name_server": p.consumerConfig.NameServer,
					},
				).Warnln("pull broker timeout")
				time.Sleep(time.Second)
			}
		}

		select {
		case <-p.stopSignal:
			{
				logrus.WithFields(logrus.Fields{
					"topic":       p.topic,
					"expression":  p.expression,
					"name-server": p.consumerConfig.NameServer,
				}).Info("Stopping pull consumer")
				p.stopSignal <- struct{}{}
				return
			}
		default:
			time.Sleep(time.Millisecond * 10)
		}
	}
}

func (p *PullConsumer) Stop() error {
	if p.stopSignal != nil {

		p.stopSignal <- struct{}{}
		<-p.stopSignal
		close(p.stopSignal)
		p.stopSignal = nil

		logrus.WithFields(logrus.Fields{
			"topic":       p.topic,
			"expression":  p.expression,
			"name-server": p.consumerConfig.NameServer,
		}).Info("Pull consumer stopped")

		return p.pullConsumer.Shutdown()
	}

	return nil
}

func NewPullConsumer(conf config.Configuration) (consumer *PullConsumer, err error) {

	consumerConf := conf.GetConfig("consumer")

	nameServer := consumerConf.GetString("name-server")
	groupID := consumerConf.GetString("group-id")
	topic := consumerConf.GetString("subscribe.topic")
	expression := consumerConf.GetString("subscribe.expression", "*")
	queueIDs := consumerConf.GetInt32List("subscribe.queue-ids")
	maxFetch := consumerConf.GetInt32("max-fetch", 32)
	instanceName := consumerConf.GetString("instance-name", "")

	if len(queueIDs) == 0 {
		err = fmt.Errorf("subscribe.queue-ids at least one id")
		return
	}

	credentialName := consumerConf.GetString("credential-name")

	if len(credentialName) == 0 {
		err = fmt.Errorf("credential name is empty")
		return
	}

	accessKey := conf.GetString("credentials." + credentialName + ".access-key")
	secretKey := conf.GetString("credentials." + credentialName + ".secret-key")
	channel := conf.GetString("credentials." + credentialName + ".channel")

	consumerConfig := &rmq.PullConsumerConfig{
		ClientConfig: rmq.ClientConfig{
			GroupID:      groupID,
			NameServer:   nameServer,
			InstanceName: instanceName,
			Credentials: &rmq.SessionCredentials{
				AccessKey: accessKey,
				SecretKey: secretKey,
				Channel:   channel,
			},
		},
	}

	pullConsumer, err := rmq.NewPullConsumer(consumerConfig)
	if err != nil {
		return
	}

	mapQueueIDs := map[int]bool{}
	for _, id := range queueIDs {
		mapQueueIDs[int(id)] = true
	}

	consumer = &PullConsumer{
		topic:      topic,
		expression: expression,
		maxFetch:   int(maxFetch),

		consumerConfig: consumerConfig,
		pullConsumer:   pullConsumer,

		queueOffsets: make(map[int]*int64),
		queueIDs:     mapQueueIDs,
	}

	return
}

func (p *PullConsumer) SetConsumerFunc(fn ConsumeFunc) {
	p.consumeFunc = fn
}
