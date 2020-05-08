package rocketmq

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"

	rmq "github.com/apache/rocketmq-client-go/core"
	"github.com/go-spirit/spirit/component"
	"github.com/go-spirit/spirit/doc"
	"github.com/go-spirit/spirit/mail"
	"github.com/go-spirit/spirit/message"
	"github.com/go-spirit/spirit/worker"
	"github.com/go-spirit/spirit/worker/fbp"
	"github.com/go-spirit/spirit/worker/fbp/protocol"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type ConsumeFunc func(msg *rmq.MessageExt) (err error)

type rocketMQConsumer interface {
	SetConsumerFunc(fn ConsumeFunc)
	Start() error
	Stop() error
}

type RocketMQComponent struct {
	opts component.Options

	consumer rocketMQConsumer

	alias string

	producers    map[string]rmq.Producer
	producerLock sync.RWMutex
}

func init() {
	component.RegisterComponent("rocketmq", NewRocketMQComponent)
	doc.RegisterDocumenter("rocketmq", &RocketMQComponent{})
}

func (p *RocketMQComponent) Alias() string {
	if p == nil {
		return ""
	}
	return p.alias
}

func (p *RocketMQComponent) Start() error {
	if p.consumer != nil {
		return p.consumer.Start()
	}
	return nil
}

func (p *RocketMQComponent) Stop() (err error) {

	if p.consumer != nil {
		err = p.consumer.Stop()
		if err != nil {
			return
		}
	}

	for _, producer := range p.producers {
		producer.Shutdown()
	}

	return
}

func NewRocketMQComponent(alias string, opts ...component.Option) (comp component.Component, err error) {

	rmqComp := &RocketMQComponent{
		alias:     alias,
		producers: make(map[string]rmq.Producer),
	}

	err = rmqComp.init(opts...)
	if err != nil {
		return
	}

	comp = rmqComp

	return
}

func (p *RocketMQComponent) init(opts ...component.Option) (err error) {

	for _, o := range opts {
		o(&p.opts)
	}

	if p.opts.Config == nil {
		err = errors.New("rocketmq component config is nil")
		return
	}

	if p.opts.Config.HasPath("consumer") {
		mode := p.opts.Config.GetString("consumer.mode", "pull")

		if mode == "pull" {
			p.consumer, err = NewPullConsumer(p.opts.Config)
			if err != nil {
				return
			}
		} else if mode == "push" {

			p.consumer, err = NewPushConsumer(p.opts.Config)
			if err != nil {
				return
			}
		} else {
			err = fmt.Errorf("unknown mode: %s (push|pull)", mode)
			return
		}

		p.consumer.SetConsumerFunc(p.postMessage)
	}

	return
}

func (p *RocketMQComponent) Route(mail.Session) worker.HandlerFunc {
	return p.sendMessage
}

type MQSendResult struct {
	Topic      string         `json:"topic"`
	GroupID    string         `json:"group_id"`
	NameServer string         `json:"name_server"`
	Tags       string         `json:"tags"`
	Keys       string         `json:"keys"`
	Status     rmq.SendStatus `json:"status"`
	MsgID      string         `json:"msg_id"`
	Offset     int64          `json:"offset"`
}

func (p *RocketMQComponent) sendMessage(session mail.Session) (err error) {

	logrus.WithField("component", "rocketmq").WithField("To", session.To()).Debugln("send message")

	setBody := session.Query("setbody")

	if len(setBody) == 0 {
		fbp.BreakSession(session)
	}

	port := fbp.GetSessionPort(session)

	if port == nil {
		err = errors.New("port info not exist")
		return
	}

	groupID := session.Query("group_id")
	topic := session.Query("topic")
	tags := session.Query("tags")

	if len(groupID) == 0 {
		err = fmt.Errorf("query of %s is empty", "group_id")
		return
	}

	if len(topic) == 0 {
		err = fmt.Errorf("query of %s is empty", "topic")
		return
	}

	if len(tags) == 0 {
		err = fmt.Errorf("query of %s is empty", "tags")
		return
	}

	nameServer := session.Query("name_server")
	accessKey := session.Query("access_key")
	secretKey := session.Query("secret_key")
	channel := session.Query("channel")
	credentialName := session.Query("credential_name")

	if len(nameServer) == 0 {
		nameServer = port.Metadata["name_server"]
	}

	if len(nameServer) == 0 {
		err = fmt.Errorf("unknown name server in rocketmq component while send message, port to url: %s", port.Url)
		return
	}

	if len(credentialName) > 0 {
		accessKey = p.opts.Config.GetString("credentials." + credentialName + ".access-key")
		secretKey = p.opts.Config.GetString("credentials." + credentialName + ".secret-key")
		channel = p.opts.Config.GetString("credentials." + credentialName + ".channel")
	}

	if len(accessKey) == 0 {
		accessKey = port.Metadata["access_key"]
	}

	if len(secretKey) == 0 {
		secretKey = port.Metadata["secret_key"]
	}

	if len(channel) == 0 {
		channel = port.Metadata["channel"]
	}

	partition := session.Query("partition")
	errorGraph := session.Query("error")
	entrypointGraph := session.Query("entrypoint")

	var sendSession mail.Session = session

	if partition == "1" {
		var newSession mail.Session
		newSession, err = fbp.PartitionFromSession(session, entrypointGraph, errorGraph)
		if err != nil {
			return
		}
		sendSession = newSession
	}

	payload, ok := sendSession.Payload().Interface().(*protocol.Payload)
	if !ok {
		err = errors.New("could not convert session payload to *protocol.Payload")
		return
	}

	data, err := payload.ToBytes()
	if err != nil {
		return
	}

	msgBody := base64.StdEncoding.EncodeToString(data)

	pConfig := &rmq.ProducerConfig{
		ClientConfig: rmq.ClientConfig{
			GroupID:    groupID,
			NameServer: nameServer,
			Credentials: &rmq.SessionCredentials{
				AccessKey: accessKey,
				SecretKey: secretKey,
				Channel:   channel,
			},
		},
		ProducerModel: rmq.CommonProducer,
	}

	propertyMap := map[string]string{}

	property, _ := port.Metadata["property"]

	if len(property) > 0 {
		err = json.Unmarshal([]byte(property), &propertyMap)
		if err != nil {
			err = errors.WithMessage(err, "property format error")
			return
		}
	}

	if mqttClientID, ok := session.Payload().Content().HeaderOf("MQTT-TO-CLIENT-ID"); ok && len(mqttClientID) > 0 {
		propertyMap["mqttSecondTopic"] = "/p2p/" + mqttClientID
	}

	msg := rmq.Message{
		Topic:    topic,
		Body:     msgBody,
		Tags:     tags,
		Property: propertyMap,
		Keys:     payload.GetId(),
	}

	sendResult, err := p.sendMessageToRMQ(pConfig, &msg)

	if err != nil {
		return
	}

	if setBody == "1" {
		err = session.Payload().Content().SetBody(
			&MQSendResult{
				Topic:      topic,
				GroupID:    groupID,
				NameServer: nameServer,
				Tags:       tags,
				Keys:       payload.GetId(),
				Status:     sendResult.Status,
				MsgID:      sendResult.MsgId,
				Offset:     sendResult.Offset,
			})
		if err != nil {
			return
		}
	}

	return
}

func (p *RocketMQComponent) getProducer(config *rmq.ProducerConfig) (ret rmq.Producer, err error) {
	key := fmt.Sprintf("%s:%s:%s", config.NameServer, config.GroupID, config.Credentials.String())

	p.producerLock.RLock()
	if producer, exist := p.producers[key]; exist {
		ret = producer
		p.producerLock.RUnlock()
		return
	}
	p.producerLock.RUnlock()

	p.producerLock.Lock()
	defer p.producerLock.Unlock()

	producer, err := rmq.NewProducer(config)
	if err != nil {
		return
	}

	err = producer.Start()
	if err != nil {
		err = errors.WithMessage(err, "start common producer error")
		return
	}

	p.producers[key] = producer

	ret = producer

	return
}

func (p *RocketMQComponent) sendMessageToRMQ(config *rmq.ProducerConfig, msg *rmq.Message) (result *rmq.SendResult, err error) {
	producer, err := p.getProducer(config)

	if err != nil {
		err = errors.WithMessage(err, "get producer failed")
		return
	}

	sendResult, err := producer.SendMessageSync(msg)
	if err != nil {
		return
	}

	result = sendResult

	logrus.WithFields(
		logrus.Fields{
			"component":   "rocketmq",
			"alias":       p.alias,
			"topic":       msg.Topic,
			"tags":        msg.Tags,
			"name_server": config.NameServer,
			"access_key":  config.Credentials.AccessKey,
			"key":         msg.Keys,
			"result":      sendResult.String(),
		},
	).Debugln("Message sent")

	return
}

func (p *RocketMQComponent) postMessage(msg *rmq.MessageExt) (err error) {

	data, err := base64.StdEncoding.DecodeString(msg.Body)
	if err != nil {
		return
	}

	payload := &protocol.Payload{}
	err = protocol.Unmarshal(data, payload)

	if err != nil {
		return
	}

	graph, exist := payload.GetGraph(payload.GetCurrentGraph())

	if !exist {
		err = fmt.Errorf("could not get graph of %s in RocketMQComponent.postMessage", payload.GetCurrentGraph())
		return
	}

	graph.MoveForward()

	port, err := graph.CurrentPort()

	if err != nil {
		return
	}

	fromUrl := ""
	prePort, preErr := graph.PrevPort()
	if preErr == nil {
		fromUrl = prePort.GetUrl()
	}

	session := mail.NewSession()

	session.WithPayload(payload)
	session.WithFromTo(fromUrl, port.GetUrl())

	fbp.SessionWithPort(session, graph.GetName(), port.GetUrl(), port.GetMetadata())

	err = p.opts.Postman.Post(
		message.NewUserMessage(session),
	)

	if err != nil {
		return
	}

	return
}

func (p *RocketMQComponent) Document() doc.Document {

	document := doc.Document{
		Title:       "RocketMQ Sender And Receiver",
		Description: "we could receive queue message from rocketmq and send message to rocketmq",
	}

	return document
}
