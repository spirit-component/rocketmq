package rocketmq

import (
	"context"
	"fmt"
	"time"

	rmq "github.com/apache/rocketmq-client-go"
	"github.com/apache/rocketmq-client-go/consumer"
	"github.com/apache/rocketmq-client-go/primitive"
	"github.com/gogap/config"
	"github.com/sirupsen/logrus"
	"github.com/spirit-component/rocketmq/queue_table"
	"golang.org/x/time/rate"
)

type PullConsumer struct {
	consumerOptions []consumer.Option
	pullConsumer    rmq.PullConsumer
	consumeFunc     ConsumeFunc

	messageSelector consumer.MessageSelector
	queueTable      queue_table.QueueTable

	topic      string
	maxFetch   int
	autoCommit bool

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
	ctxPull := context.Background()

	for {
		for _, mq := range p.queueTable.Queues() {

			pullResult, err := p.pullConsumer.Pull(ctxPull, p.topic, p.maxFetch)
			if err != nil {
				if err == rmq.ErrRequestTimeout {
					logrus.Errorln(err)
					time.Sleep(1 * time.Second)
					continue
				}
				logrus.Errorln(err)
				continue
			}

			switch pullResult.Status {
			case primitive.PullNoNewMsg:
			case primitive.PullFound:
				{
					if !p.autoCommit {
						_, err := p.pullConsumer.Commit(ctxPull, mq)
						if err != nil {
							logrus.Errorln(err)
							continue
						}
					}

					pulledMessages := pullResult.GetMessageExts()

					for i := 0; i < len(pulledMessages); i++ {

						p.limiter.Wait(ctx)

						err := p.consumeFunc(pulledMessages[i])
						if err != nil {
							logrus.Errorln(err)
							err = p.pullConsumer.Seek(mq, pulledMessages[i].QueueOffset)
							if err != nil {
								logrus.Errorln(err)
							}
							break
						}
					}
				}
			case primitive.PullNoMsgMatched:
				if !p.autoCommit {
					p.pullConsumer.Commit(ctxPull, mq)
				}
			case primitive.PullOffsetIllegal:
				if !p.autoCommit {
					p.pullConsumer.Commit(ctxPull, mq)
				}
			case primitive.PullBrokerTimeout:
				logrus.WithFields(
					logrus.Fields{
						"topic":      p.topic,
						"expression": p.messageSelector.Expression,
						"broker":     mq.BrokerName,
					},
				).Warnln("pull broker timeout")
				time.Sleep(time.Second)
			}
		}

		select {
		case <-p.stopSignal:
			{
				logrus.WithFields(logrus.Fields{
					"topic":      p.topic,
					"expression": p.messageSelector.Expression,
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
			"topic":      p.topic,
			"expression": p.messageSelector.Expression,
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

func NewPullConsumer(conf config.Configuration) (ret *PullConsumer, err error) {

	consumerConf := conf.GetConfig("consumer")

	vipChannel := consumerConf.GetBoolean("vip-channel", false)
	nameServer := consumerConf.GetStringList("name-server")
	nameServerDomain := consumerConf.GetString("name-server-domain")
	namespace := consumerConf.GetString("namespace")
	groupID := consumerConf.GetString("group-id")
	topic := consumerConf.GetString("subscribe.topic")
	expression := consumerConf.GetString("subscribe.expression", "*")
	expressionType := consumerConf.GetString("subscribe.expression-type", "TAG")
	maxFetch := consumerConf.GetInt32("max-fetch", 32)
	autoCommit := consumerConf.GetBoolean("auto-commit", true)
	instanceName := consumerConf.GetString("instance-name", "")

	credentialName := consumerConf.GetString("credential-name")

	if len(credentialName) == 0 {
		err = fmt.Errorf("credential name is empty")
		return
	}

	accessKey := conf.GetString("credentials." + credentialName + ".access-key")
	secretKey := conf.GetString("credentials." + credentialName + ".secret-key")
	securityToken := conf.GetString("credentials." + credentialName + ".channel")

	pullConsumer, err := rmq.NewPullConsumer(
		consumer.WithInstance(instanceName),
		consumer.WithVIPChannel(vipChannel),
		consumer.WithGroupName(groupID),
		consumer.WithNameServer(nameServer),
		consumer.WithNameServerDomain(nameServerDomain),
		consumer.WithNamespace(namespace),
		consumer.WithCredentials(
			primitive.Credentials{
				AccessKey:     accessKey,
				SecretKey:     secretKey,
				SecurityToken: securityToken,
			}),
		consumer.WithPullBatchSize(maxFetch),
		consumer.WithAutoCommit(autoCommit),
	)

	if err != nil {
		return
	}

	queueTableConfig := consumerConf.GetConfig("subscribe.queue-table")
	queueTableProvider := queueTableConfig.GetString("provider", "in-memory")

	queueTable, err := queue_table.NewQueueTable(queueTableProvider, pullConsumer, topic, expression, instanceName, queueTableConfig)
	if err != nil {
		return
	}

	qps := consumerConf.GetFloat64("rate-limit.qps", 1000)
	bucketSize := consumerConf.GetInt32("rate-limit.bucket-size", 1)

	logrus.WithFields(
		logrus.Fields{
			"topic":         topic,
			"expression":    expression,
			"instance_name": instanceName,
			"qps":           qps,
			"bucket_size":   bucketSize,
		},
	).Debug("rate limit configured")

	messageSelector := consumer.MessageSelector{
		Type:       consumer.ExpressionType(expressionType),
		Expression: expression,
	}

	ret = &PullConsumer{

		topic:           topic,
		messageSelector: messageSelector,
		maxFetch:        int(maxFetch),

		pullConsumer: pullConsumer,
		autoCommit:   autoCommit,

		queueTable: queueTable,
		limiter:    rate.NewLimiter(rate.Limit(qps), int(bucketSize)),
	}

	return
}

func (p *PullConsumer) SetConsumerFunc(fn ConsumeFunc) {
	p.consumeFunc = fn
}
