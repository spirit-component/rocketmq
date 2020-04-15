package rocketmq

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	rmq "github.com/apache/rocketmq-client-go"
	"github.com/apache/rocketmq-client-go/primitive"
	"github.com/go-spirit/spirit/component"
	"github.com/go-spirit/spirit/doc"
	"github.com/go-spirit/spirit/mail"
	"github.com/go-spirit/spirit/message"
	"github.com/go-spirit/spirit/worker"
	"github.com/go-spirit/spirit/worker/fbp"
	"github.com/go-spirit/spirit/worker/fbp/protocol"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/apache/rocketmq-client-go/producer"
)

type ConsumeFunc func(msg *primitive.MessageExt) (err error)

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
	return p.consumer.Start()
}

func (p *RocketMQComponent) Stop() (err error) {

	err = p.consumer.Stop()
	if err != nil {
		return
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

	return
}

func (p *RocketMQComponent) Route(mail.Session) worker.HandlerFunc {
	return p.sendMessage
}

type MQSendResult struct {
	Topic      string `json:"topic"`
	GroupID    string `json:"group_id"`
	NameServer string `json:"name_server"`
	Tags       string `json:"tags"`
	Keys       string `json:"keys"`
	Status     int    `json:"status"`
	MsgID      string `json:"msg_id"`
	Offset     int64  `json:"offset"`
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

	nameServer := session.Query("name_server")
	nameServerDomain := session.Query("name_server_domain")
	accessKey := session.Query("access_key")
	secretKey := session.Query("secret_key")
	channel := session.Query("channel")
	namespace := session.Query("namespace")
	vipChannel := session.Query("vip_channel")
	sendTimeout := session.Query("send_timeout")
	retryTimes := session.Query("retry")
	credentialName := session.Query("credential_name")
	delayLevel := session.Query("delay_level")

	if len(nameServer) == 0 {
		nameServer = port.Metadata["name_server"]
	}

	if len(nameServerDomain) == 0 {
		nameServerDomain = port.Metadata["name_server_domain"]
	}

	if len(nameServer) == 0 && len(nameServerDomain) == 0 {
		err = fmt.Errorf("unknown name server or domain in rocketmq component while send message, port to url: %s", port.Url)
		return
	}

	nameServerList := strings.Split(nameServer, ",")

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

	if len(namespace) == 0 {
		namespace = port.Metadata["namespace"]
	}

	if len(vipChannel) == 0 {
		vipChannel = port.Metadata["vip_channel"]
	}

	if len(sendTimeout) == 0 {
		sendTimeout = port.Metadata["send_timeout"]
	}

	if len(sendTimeout) == 0 {
		sendTimeout = "5s"
	}

	if len(retryTimes) == 0 {
		retryTimes = port.Metadata["retries"]
	}

	if len(delayLevel) == 0 {
		delayLevel = port.Metadata["delay-level"]
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

	durSendTimeout, _ := time.ParseDuration(sendTimeout)
	retries, _ := strconv.Atoi(retryTimes)
	lvl, _ := strconv.Atoi(delayLevel)

	msgBody := base64.StdEncoding.EncodeToString(data)

	var producerOptions []producer.Option
	producerOptions = append(
		producerOptions,
		producer.WithCreateTopicKey(topic),
		producer.WithGroupName(groupID),
		producer.WithNameServer(nameServerList),
		producer.WithNameServerDomain(nameServerDomain),
		producer.WithNamespace(namespace),
		producer.WithVIPChannel(vipChannel == "1"),
		producer.WithSendMsgTimeout(durSendTimeout),
		producer.WithRetry(retries),
	)

	propertyMap := map[string]string{}

	property, _ := port.Metadata["property"]

	if len(property) > 0 {
		err = json.Unmarshal([]byte(property), &propertyMap)
		if err != nil {
			err = errors.WithMessage(err, "property format error")
			return
		}
	}

	msg := primitive.NewMessage(topic, []byte(msgBody))
	msg.WithProperties(propertyMap)
	msg.WithTag(tags)
	msg.WithKeys([]string{payload.GetId()})
	msg.WithDelayTimeLevel(lvl)

	producerKey := fmt.Sprintf("%s:%s:%s:%s", nameServer, nameServerDomain, topic, secretKey)

	sendResult, err := p.sendMessageToRMQ(producerKey, msg, producerOptions)

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
				Status:     int(sendResult.Status),
				MsgID:      sendResult.MsgID,
				Offset:     sendResult.QueueOffset,
			})
		if err != nil {
			return
		}
	}

	return
}

func (p *RocketMQComponent) getProducer(key string, options ...producer.Option) (ret rmq.Producer, err error) {

	p.producerLock.RLock()
	if producer, exist := p.producers[key]; exist {
		ret = producer
		p.producerLock.RUnlock()
		return
	}
	p.producerLock.RUnlock()

	p.producerLock.Lock()
	defer p.producerLock.Unlock()

	producer, err := rmq.NewProducer(options...)
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

func (p *RocketMQComponent) sendMessageToRMQ(producerKey string, msg *primitive.Message, options []producer.Option) (result *primitive.SendResult, err error) {
	producer, err := p.getProducer(producerKey, options...)

	if err != nil {
		err = errors.WithMessage(err, "get producer failed")
		return
	}

	sendResult, err := producer.SendSync(context.Background(), msg)
	if err != nil {
		return
	}

	result = sendResult

	logrus.WithFields(
		logrus.Fields{
			"component":  "rocketmq",
			"alias":      p.alias,
			"topic":      msg.Topic,
			"properties": msg.MarshallProperties(),
			"tags":       msg.GetTags(),
			"keys":       msg.GetKeys(),
			"producer":   producerKey,
			"result":     sendResult.String(),
		},
	).Debugln("Message sent")

	return
}

func (p *RocketMQComponent) postMessage(msg *primitive.MessageExt) (err error) {

	data, err := base64.StdEncoding.DecodeString(string(msg.Body))
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
