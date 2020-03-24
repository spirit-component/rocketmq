package rocketmq

import (
	"fmt"
	"github.com/google/uuid"
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

	messageChan  chan<- *rmq.MessageExt
	queues       []rmq.MessageQueue
	queueOffsets map[int]*int64

	stopSignal chan struct{}
}

func (p *PullConsumer) Start() (err error) {
	err = p.pullConsumer.Start()
	if err != nil {
		return
	}

	p.queues = p.pullConsumer.FetchSubscriptionMessageQueues(p.topic)

	for i := 0; i < len(p.queues); i++ {
		p.queueOffsets[p.queues[i].ID] = new(int64)
	}

	p.stopSignal = make(chan struct{})

	go p.pull()

	return
}

func (p *PullConsumer) pull() {

	for {
		for _, mq := range p.queues {
			pullResult := p.pullConsumer.Pull(mq, p.expression, atomic.LoadInt64(p.queueOffsets[mq.ID]), p.maxFetch)

			if pullResult.NextBeginOffset < pullResult.MaxOffset {
				atomic.StoreInt64(p.queueOffsets[mq.ID], pullResult.MaxOffset)
				continue
			}

			switch pullResult.Status {
			case rmq.PullNoNewMsg:
			case rmq.PullFound:
				{

					atomic.StoreInt64(p.queueOffsets[mq.ID], pullResult.NextBeginOffset)

					for i := 0; i < len(pullResult.Messages); i++ {
						p.consumeFunc(pullResult.Messages[i])
						//TODO: process error
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

func NewPullConsumer(messageChan chan<- *rmq.MessageExt, conf config.Configuration) (consumer *PullConsumer, err error) {

	consumerConf := conf.GetConfig("consumer")

	nameServer := consumerConf.GetString("name-server")
	groupID := consumerConf.GetString("group-id")
	topic := consumerConf.GetString("subscribe.topic")
	expression := consumerConf.GetString("subscribe.expression", "*")
	maxFetch := consumerConf.GetInt32("max-fetch", 32)

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
			InstanceName: uuid.New().String(),
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

	consumer = &PullConsumer{
		topic:      topic,
		expression: expression,
		maxFetch:   int(maxFetch),

		consumerConfig: consumerConfig,
		pullConsumer:   pullConsumer,
		messageChan:    messageChan,

		queueOffsets: make(map[int]*int64),
	}

	return
}

func (p *PullConsumer) SetConsumerFunc(fn ConsumeFunc) {
	p.consumeFunc = fn
}
