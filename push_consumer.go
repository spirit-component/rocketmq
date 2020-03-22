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

	messageChan chan<- *rmq.MessageExt
}

func (p *PushConsumer) Start() (err error) {

	err = p.pushConsumer.Subscribe(p.topic, p.expression, p.consumeFunc)
	if err != nil {
		return
	}

	err = p.pushConsumer.Start()
	if err != nil {
		return
	}

	return
}

func (p *PushConsumer) consumeFunc(msg *rmq.MessageExt) rmq.ConsumeStatus {
	p.messageChan <- msg
	return rmq.ConsumeSuccess
}

func (p *PushConsumer) Stop() error {
	return p.pushConsumer.Shutdown()
}

func NewPushConsumer(messageChan chan<- *rmq.MessageExt, conf config.Configuration) (consumer *PushConsumer, err error) {

	nameServer := conf.GetString("name-server")
	groupID := conf.GetString("group-id")

	topic := conf.GetString("subscribe.topic")
	expression := conf.GetString("subscribe.expression", "*")

	credentialName := conf.GetString("credential-name")

	if len(credentialName) == 0 {
		err = fmt.Errorf("credential name is empty")
		return
	}

	accessKey := conf.GetString("credentials." + credentialName + ".access-key")
	secretKey := conf.GetString("credentials." + credentialName + ".secret-key")
	channel := conf.GetString("credentials." + credentialName + ".channel")

	var messageModel rmq.MessageModel
	switch conf.GetString("message-model", "clustering") {
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
	switch conf.GetString("consumer-model", "cocurrently") {
	case "cocurrently":
		{
			consumerModel = rmq.CoCurrently
		}
	case "orderly":
		{
			consumerModel = rmq.Orderly
		}
	}

	threadCount := conf.GetInt32("thread-count", int32(runtime.NumCPU()))
	messageBatchMaxSize := conf.GetInt32("msg-batch-max-size", 32)
	maxCacheMsgSize := conf.GetByteSize("max-cache-msg-size")

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

		consumerConfig: consumerConfig,
		pushConsumer:   pushConsumer,
		messageChan:    messageChan,
	}

	return
}
