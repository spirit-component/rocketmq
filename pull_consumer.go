package rocketmq

import (
	"fmt"
	"time"

	rmq "github.com/apache/rocketmq-client-go/core"
	"github.com/gogap/config"
	"github.com/sirupsen/logrus"
	"github.com/spirit-component/rocketmq/queue_table"

	_ "github.com/spirit-component/rocketmq/queue_table/inmemory"
)

type PullConsumer struct {
	consumerConfig *rmq.PullConsumerConfig
	pullConsumer   rmq.PullConsumer
	consumeFunc    ConsumeFunc

	topic      string
	expression string
	maxFetch   int

	queueTable queue_table.QueueTable

	stopSignal chan struct{}
}

func (p *PullConsumer) Start() (err error) {
	err = p.pullConsumer.Start()
	if err != nil {
		return
	}

	err = p.queueTable.Start()
	if err != nil {
		return
	}

	p.stopSignal = make(chan struct{})

	go p.pull()

	return
}

func (p *PullConsumer) pull() {

	for {
		for _, mq := range p.queueTable.Queues() {
			offset, errGetOffset := p.queueTable.CurrentOffset(mq.ID)
			if errGetOffset != nil {
				logrus.Errorln(errGetOffset)
				break
			}
			pullResult := p.pullConsumer.Pull(mq, p.expression, offset, p.maxFetch)

			switch pullResult.Status {
			case rmq.PullNoNewMsg:
			case rmq.PullFound:
				{

					err := p.queueTable.UpdateOffset(mq.ID, pullResult.NextBeginOffset)
					if err != nil {
						logrus.Errorln(err)
						break
					}

					for i := 0; i < len(pullResult.Messages); i++ {
						err := p.consumeFunc(pullResult.Messages[i])
						if err != nil {
							logrus.Errorln(err)
							err = p.queueTable.UpdateOffset(mq.ID, pullResult.Messages[i].QueueOffset)
							if err != nil {
								logrus.Errorln(err)
							}
							break
						}
					}
				}
			case rmq.PullNoMatchedMsg:
				p.queueTable.UpdateOffset(mq.ID, pullResult.NextBeginOffset)
			case rmq.PullOffsetIllegal:
				p.queueTable.UpdateOffset(mq.ID, pullResult.NextBeginOffset)
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
				}).Info("stopping pull consumer")
				p.stopSignal <- struct{}{}
				return
			}
		default:
			time.Sleep(time.Millisecond * 10)
		}
	}
}

func (p *PullConsumer) Stop() (err error) {

	err = p.queueTable.Stop()
	if err != nil {
		return
	}

	if p.stopSignal != nil {

		p.stopSignal <- struct{}{}
		<-p.stopSignal
		close(p.stopSignal)
		p.stopSignal = nil

		logrus.WithFields(logrus.Fields{
			"topic":       p.topic,
			"expression":  p.expression,
			"name-server": p.consumerConfig.NameServer,
		}).Info("pull consumer stopped")

		err = p.queueTable.Stop()
		if err != nil {
			return
		}

		err = p.pullConsumer.Shutdown()
		if err != nil {
			return
		}
	}

	return nil
}

func NewPullConsumer(conf config.Configuration) (consumer *PullConsumer, err error) {

	consumerConf := conf.GetConfig("consumer")

	nameServer := consumerConf.GetString("name-server")
	groupID := consumerConf.GetString("group-id")
	topic := consumerConf.GetString("subscribe.topic")
	expression := consumerConf.GetString("subscribe.expression", "*")
	maxFetch := consumerConf.GetInt32("max-fetch", 32)
	instanceName := consumerConf.GetString("instance-name", "")

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

	queueTableConfig := consumerConf.GetConfig("subscribe.queue-table")
	queueTableProvider := queueTableConfig.GetString("provider", "in-memory")

	queueTable, err := queue_table.NewQueueTable(queueTableProvider, pullConsumer, topic, expression, consumerConfig, queueTableConfig)
	if err != nil {
		return
	}

	consumer = &PullConsumer{
		topic:      topic,
		expression: expression,
		maxFetch:   int(maxFetch),

		consumerConfig: consumerConfig,
		pullConsumer:   pullConsumer,

		queueTable: queueTable,
	}

	return
}

func (p *PullConsumer) SetConsumerFunc(fn ConsumeFunc) {
	p.consumeFunc = fn
}
