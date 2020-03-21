package rocketmq

import (
	"github.com/go-spirit/spirit/component"
	"github.com/go-spirit/spirit/doc"
	"github.com/go-spirit/spirit/mail"
	"github.com/go-spirit/spirit/worker"
)

type RocketMQComponent struct {
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

	return nil
}

func (p *RocketMQComponent) Stop() error {
	return nil
}

func NewRocketMQComponent(alias string, opts ...component.Option) (comp component.Component, err error) {
	return
}

func (p *RocketMQComponent) Route(mail.Session) worker.HandlerFunc {
	return p.sendMessage
}

func (p *RocketMQComponent) sendMessage(session mail.Session) (err error) {
	return
}

func (p *RocketMQComponent) receiveMessage() {
}

func (p *RocketMQComponent) Document() doc.Document {

	document := doc.Document{
		Title:       "RocketMQ Sender And Receiver",
		Description: "we could receive queue message from rocketmq and send message to rocketmq",
	}

	return document
}
