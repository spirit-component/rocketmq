package rocketmq

import (
	"fmt"
	"sync/atomic"

	rmq "github.com/apache/rocketmq-client-go/core"
	"github.com/gogap/config"
	"github.com/gogap/logrus"
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

	p.stopSignal = make(chan struct{}, 1)

	go p.pull()

	return
}

func (p *PullConsumer) pull() {

	for {
		for _, mq := range p.queues {
			pullResult := p.pullConsumer.Pull(mq, p.expression, atomic.LoadInt64(p.queueOffsets[mq.ID]), p.maxFetch)

			switch pullResult.Status {
			case rmq.PullNoNewMsg:
				continue
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
				continue
			}
		}

		select {
		case <-p.stopSignal:
			{
				p.stopSignal <- struct{}{}
				return
			}
		default:
		}
	}
}

func (p *PullConsumer) Stop() error {
	if p.stopSignal != nil {

		p.stopSignal <- struct{}{}
		<-p.stopSignal
		close(p.stopSignal)
		p.stopSignal = nil

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
			GroupID:    groupID,
			NameServer: nameServer,
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
