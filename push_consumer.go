package rocketmq

import (
	"fmt"
	"runtime"

	rmq "github.com/apache/rocketmq-client-go/core"
	"github.com/gogap/config"
)

type PushConsumer struct {
	consumerConfig *rmq.PushConsumerConfig
	pushConsumer   rmq.PushConsumer

	topic      string
	expression string
	retryTimes int

	messageChan chan<- *rmq.MessageExt

	consumeFunc ConsumeFunc
}

func (p *PushConsumer) Start() (err error) {

	err = p.pushConsumer.Subscribe(p.topic, p.expression, p.consume)
	if err != nil {
		return
	}

	err = p.pushConsumer.Start()
	if err != nil {
		return
	}

	return
}

func (p *PushConsumer) consume(msg *rmq.MessageExt) rmq.ConsumeStatus {
	err := p.consumeFunc(msg)
	if err != nil {
		if msg.ReconsumeTimes < p.retryTimes {
			return rmq.ReConsumeLater
		}
	}
	return rmq.ConsumeSuccess
}

func (p *PushConsumer) Stop() error {
	return p.pushConsumer.Shutdown()
}

func NewPushConsumer(messageChan chan<- *rmq.MessageExt, conf config.Configuration) (consumer *PushConsumer, err error) {

	consumerConf := conf.GetConfig("consumer")

	nameServer := consumerConf.GetString("name-server")
	groupID := consumerConf.GetString("group-id")

	topic := consumerConf.GetString("subscribe.topic")
	expression := consumerConf.GetString("subscribe.expression", "*")
	retryTimes := consumerConf.GetInt32("subscribe.retry-times", 0)

	credentialName := consumerConf.GetString("credential-name")

	if len(credentialName) == 0 {
		err = fmt.Errorf("credential name is empty")
		return
	}

	accessKey := conf.GetString("credentials." + credentialName + ".access-key")
	secretKey := conf.GetString("credentials." + credentialName + ".secret-key")
	channel := conf.GetString("credentials." + credentialName + ".channel")

	var messageModel rmq.MessageModel
	switch consumerConf.GetString("message-model", "clustering") {
	case "clustering":
		{
			messageModel = rmq.Clustering
		}
	case "broadcasting":
		{
			messageModel = rmq.BroadCasting
		}
	}

	var consumerModel rmq.ConsumerModel
	switch consumerConf.GetString("consumer-model", "cocurrently") {
	case "cocurrently":
		{
			consumerModel = rmq.CoCurrently
		}
	case "orderly":
		{
			consumerModel = rmq.Orderly
		}
	}

	threadCount := consumerConf.GetInt32("thread-count", int32(runtime.NumCPU()))
	messageBatchMaxSize := consumerConf.GetInt32("msg-batch-max-size", 32)
	maxCacheMsgSize := consumerConf.GetByteSize("max-cache-msg-size")

	consumerConfig := &rmq.PushConsumerConfig{
		ClientConfig: rmq.ClientConfig{
			GroupID:    groupID,
			NameServer: nameServer,
			Credentials: &rmq.SessionCredentials{
				AccessKey: accessKey,
				SecretKey: secretKey,
				Channel:   channel,
			},
		},
		ConsumerModel:       consumerModel,
		Model:               messageModel,
		ThreadCount:         int(threadCount),
		MessageBatchMaxSize: int(messageBatchMaxSize),
		MaxCacheMessageSize: int(maxCacheMsgSize.Int64()),
	}

	pushConsumer, err := rmq.NewPushConsumer(consumerConfig)
	if err != nil {
		return
	}

	consumer = &PushConsumer{
		topic:      topic,
		expression: expression,
		retryTimes: int(retryTimes),

		consumerConfig: consumerConfig,
		pushConsumer:   pushConsumer,
		messageChan:    messageChan,
	}

	return
}

func (p *PushConsumer) SetConsumerFunc(fn ConsumeFunc) {
	p.consumeFunc = fn
}
