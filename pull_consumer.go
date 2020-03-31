package rocketmq

import (
	"context"
	"fmt"
	"time"

	rmq "github.com/apache/rocketmq-client-go/core"
	"github.com/gogap/config"
	"github.com/sirupsen/logrus"
	"github.com/spirit-component/rocketmq/queue_table"
	"golang.org/x/time/rate"

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

	limiter *rate.Limiter
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

	ctx, _ := context.WithCancel(context.TODO())

	for {
		for _, mq := range p.queueTable.Queues() {
			offset, errGetOffset := p.queueTable.CurrentOffset(mq.Broker, mq.ID)
			if errGetOffset != nil {
				logrus.Errorln(errGetOffset)
				continue
			}
			pullResult := p.pullConsumer.Pull(mq, p.expression, offset, p.maxFetch)

			switch pullResult.Status {
			case rmq.PullNoNewMsg:
			case rmq.PullFound:
				{

					err := p.queueTable.UpdateOffset(mq.Broker, mq.ID, pullResult.NextBeginOffset)
					if err != nil {
						logrus.Errorln(err)
						continue
					}

					for i := 0; i < len(pullResult.Messages); i++ {

						p.limiter.Wait(ctx)

						err := p.consumeFunc(pullResult.Messages[i])
						if err != nil {
							logrus.Errorln(err)
							err = p.queueTable.UpdateOffset(mq.Broker, mq.ID, pullResult.Messages[i].QueueOffset)
							if err != nil {
								logrus.Errorln(err)
							}
							break
						}
					}
				}
			case rmq.PullNoMatchedMsg:
				p.queueTable.UpdateOffset(mq.Broker, mq.ID, pullResult.NextBeginOffset)
			case rmq.PullOffsetIllegal:
				p.queueTable.UpdateOffset(mq.Broker, mq.ID, pullResult.NextBeginOffset)
			case rmq.PullBrokerTimeout:
				logrus.WithFields(
					logrus.Fields{
						"topic":       p.topic,
						"expression":  p.expression,
						"broker":      mq.Broker,
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
					"name_server": p.consumerConfig.NameServer,
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

	if p.stopSignal != nil {

		p.stopSignal <- struct{}{}
		<-p.stopSignal
		close(p.stopSignal)
		p.stopSignal = nil

		logrus.WithFields(logrus.Fields{
			"topic":       p.topic,
			"expression":  p.expression,
			"name_server": p.consumerConfig.NameServer,
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

	err = p.queueTable.Stop()
	if err != nil {
		return
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

	qps := consumerConf.GetFloat64("rate-limit.qps", 1000)
	bucketSize := consumerConf.GetInt32("rate-limit.bucket-size", 1)

	logrus.WithFields(
		logrus.Fields{
			"topic":         topic,
			"expression":    expression,
			"name_server":   consumerConfig.NameServer,
			"instance_name": instanceName,
			"qps":           qps,
			"bucket_size":   bucketSize,
		},
	).Debug("rate limit configured")

	consumer = &PullConsumer{
		topic:      topic,
		expression: expression,
		maxFetch:   int(maxFetch),

		consumerConfig: consumerConfig,
		pullConsumer:   pullConsumer,

		queueTable: queueTable,
		limiter:    rate.NewLimiter(rate.Limit(qps), int(bucketSize)),
	}

	return
}

func (p *PullConsumer) SetConsumerFunc(fn ConsumeFunc) {
	p.consumeFunc = fn
}
