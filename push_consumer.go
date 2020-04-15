package rocketmq

import (
	"context"
	"fmt"

	rmq "github.com/apache/rocketmq-client-go"
	"github.com/apache/rocketmq-client-go/consumer"
	"github.com/apache/rocketmq-client-go/primitive"
	"github.com/gogap/config"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

type PushConsumer struct {
	consumerOptions []consumer.Option
	pushConsumer    rmq.PushConsumer

	topic           string
	messageSelector consumer.MessageSelector
	retryTimes      int32

	consumeFunc ConsumeFunc

	limiter *rate.Limiter
}

func (p *PushConsumer) Start() (err error) {

	err = p.pushConsumer.Subscribe(p.topic, p.messageSelector, p.consume)
	if err != nil {
		return
	}

	err = p.pushConsumer.Start()
	if err != nil {
		return
	}

	return
}

func (p *PushConsumer) consume(ctx context.Context, messages ...*primitive.MessageExt) (result consumer.ConsumeResult, err error) {

	if !p.limiter.Allow() {
		return consumer.ConsumeRetryLater, nil
	}

	if len(messages) != 1 {
		err = fmt.Errorf("could not consume message greater than 1")
		return
	}

	err = p.consumeFunc(messages[0])

	if err != nil {
		logrus.WithFields(
			logrus.Fields{
				"topic":           p.topic,
				"expression":      p.messageSelector.Expression,
				"keys":            messages[0].GetKeys(),
				"message_id":      messages[0].MsgId,
				"tags":            messages[0].GetTags(),
				"reconsume_times": messages[0].ReconsumeTimes,
				"born_host":       messages[0].BornHost,
			},
		).Errorln(err)

		if p.retryTimes == -1 {
			return consumer.ConsumeRetryLater, nil
		} else if messages[0].ReconsumeTimes < p.retryTimes {
			return consumer.ConsumeRetryLater, nil
		}
		return consumer.ConsumeSuccess, nil
	}

	return consumer.ConsumeSuccess, nil
}

func (p *PushConsumer) Stop() error {
	return p.pushConsumer.Shutdown()
}

func NewPushConsumer(conf config.Configuration) (ret *PushConsumer, err error) {

	consumerConf := conf.GetConfig("consumer")

	nameServer := consumerConf.GetStringList("name-server")
	nameServerDomain := consumerConf.GetString("name-server-domain")
	groupID := consumerConf.GetString("group-id")

	vipChannel := consumerConf.GetBoolean("vip-channel", false)
	topic := consumerConf.GetString("subscribe.topic")
	expression := consumerConf.GetString("subscribe.expression", "*")
	expressionType := consumerConf.GetString("subscribe.expression-type", "TAG")
	retryTimes := consumerConf.GetInt32("subscribe.retry-times", 0) // -1 always retry
	autoCommit := consumerConf.GetBoolean("auto-commit", true)
	instanceName := consumerConf.GetString("instance-name", "")
	namespace := consumerConf.GetString("namespace")

	credentialName := consumerConf.GetString("credential-name")

	if len(credentialName) == 0 {
		err = fmt.Errorf("credential name is empty")
		return
	}

	accessKey := conf.GetString("credentials." + credentialName + ".access-key")
	secretKey := conf.GetString("credentials." + credentialName + ".secret-key")
	channel := conf.GetString("credentials." + credentialName + ".channel")

	var messageModel consumer.MessageModel
	switch consumerConf.GetString("message-model", "clustering") {
	case "clustering":
		{
			messageModel = consumer.Clustering
		}
	case "broadcasting":
		{
			messageModel = consumer.BroadCasting
		}
	}

	withConsumerOrder := false
	switch consumerConf.GetString("consumer-model", "cocurrently") {
	case "cocurrently":
		{
			withConsumerOrder = false
		}
	case "orderly":
		{
			withConsumerOrder = true
		}
	}

	// messageBatchMaxSize := consumerConf.GetInt32("msg-batch-max-size", 1)
	maxCacheMsgSize := consumerConf.GetByteSize("max-cache-msg-size").Int64()

	if maxCacheMsgSize < 0 {
		maxCacheMsgSize = 0
	}

	pushConsumer, err := rmq.NewPushConsumer(
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
				SecurityToken: channel,
			}),
		consumer.WithConsumeMessageBatchMaxSize(1), // TODO: use real size
		consumer.WithAutoCommit(autoCommit),
		consumer.WithConsumerOrder(withConsumerOrder),
		consumer.WithConsumerModel(messageModel),
		consumer.WithConsumeFromWhere(consumer.ConsumeFromLastOffset), // TODO: make configureable
	)

	if err != nil {
		return
	}

	qps := consumerConf.GetFloat64("rate-limit.qps", 1000)
	bucketSize := consumerConf.GetInt32("rate-limit.bucket-size", 1)

	logrus.WithFields(
		logrus.Fields{
			"topic":       topic,
			"expression":  expression,
			"qps":         qps,
			"bucket_size": bucketSize,
		},
	).Debug("rate limit configured")

	messageSelector := consumer.MessageSelector{
		Type:       consumer.ExpressionType(expressionType),
		Expression: expression,
	}

	ret = &PushConsumer{
		topic:           topic,
		messageSelector: messageSelector,
		retryTimes:      retryTimes,
		pushConsumer:    pushConsumer,
		limiter:         rate.NewLimiter(rate.Limit(qps), int(bucketSize)),
	}

	return
}

func (p *PushConsumer) SetConsumerFunc(fn ConsumeFunc) {
	p.consumeFunc = fn
}
