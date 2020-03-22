package rocketmq

import (
	"encoding/base64"
	"errors"
	"fmt"

	rmq "github.com/apache/rocketmq-client-go/core"
	"github.com/go-spirit/spirit/component"
	"github.com/go-spirit/spirit/doc"
	"github.com/go-spirit/spirit/mail"
	"github.com/go-spirit/spirit/message"
	"github.com/go-spirit/spirit/worker"
	"github.com/go-spirit/spirit/worker/fbp"
	"github.com/go-spirit/spirit/worker/fbp/protocol"
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

	boundedMsgBox chan *rmq.MessageExt

	stopSignal chan struct{}

	alias string
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

func (p *RocketMQComponent) Stop() error {

	if p.stopSignal != nil {
		p.stopSignal <- struct{}{}
		<-p.stopSignal
		close(p.stopSignal)
		p.stopSignal = nil
	}

	return p.consumer.Stop()
}

func NewRocketMQComponent(alias string, opts ...component.Option) (comp component.Component, err error) {

	rmqComp := &RocketMQComponent{
		alias:      alias,
		stopSignal: make(chan struct{}),
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

	mode := p.opts.Config.GetString("mode", "pull")
	boxSize := p.opts.Config.GetInt32("bounded-msgbox-size", 30)

	boundedMsgBox := make(chan *rmq.MessageExt, int(boxSize))

	if mode == "pull" {
		p.consumer, err = NewPullConsumer(boundedMsgBox, p.opts.Config)
		if err != nil {
			return
		}
	} else if mode == "push" {
		p.consumer, err = NewPushConsumer(boundedMsgBox, p.opts.Config)
		if err != nil {
			return
		}
	} else {
		err = fmt.Errorf("unknown mode: %s (push|pull)", mode)
		return
	}

	p.boundedMsgBox = boundedMsgBox

	return
}

func (p *RocketMQComponent) Route(mail.Session) worker.HandlerFunc {
	return p.sendMessage
}

func (p *RocketMQComponent) sendMessage(session mail.Session) (err error) {
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
