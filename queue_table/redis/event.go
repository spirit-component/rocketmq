package redis

import (
	"encoding/json"
	"net/url"
	"strings"

	"github.com/gomodule/redigo/redis"
	"github.com/sirupsen/logrus"
)

type Event struct {
	Name string     `json:name`
	Args url.Values `json:"args"`
}

func (p *Event) Marshal() (data []byte, err error) {
	return json.Marshal(p)
}

func (p *Event) Parse(data []byte) (err error) {
	return json.Unmarshal(data, p)
}

func (p *RedisQueueTable) startEventListen() (err error) {
	conn := p.redisPool.Get()

	psc := redis.PubSubConn{Conn: conn}

	if err := psc.Subscribe(p.eventChannel); err != nil {
		return err
	}

	p.pubSubChannel = &psc

	logrus.WithFields(
		logrus.Fields{
			"topic":      p.topic,
			"expression": p.expression,
			"provider":   "redis",
			"channel":    p.eventChannel,
		},
	).Debugln("redis event listening")

	go func() {
		for {
			switch n := psc.Receive().(type) {
			case error:
				if !strings.Contains(n.Error(), "timeout") {
					logrus.WithFields(logrus.Fields{
						"topic":      p.topic,
						"expression": p.expression,
						"provider":   "redis",
					}).Errorln(err)
				}
			case redis.Message:
				if err := p.onChannelMessage(n.Channel, n.Data); err != nil {
					logrus.Errorln(err)
					return
				}
			case redis.Subscription:
				switch n.Count {
				case 0:
					logrus.WithFields(logrus.Fields{
						"topic":      p.topic,
						"expression": p.expression,
						"provider":   "redis",
					}).Info("redis event listen stopped")
					return
				}
			}
		}
	}()

	return
}

func (p *RedisQueueTable) onChannelMessage(channel string, data []byte) (err error) {

	if channel != p.eventChannel {
		return
	}

	if len(data) == 0 {
		return
	}

	event := &Event{}

	e := event.Parse(data)
	if e != nil {
		logrus.WithError(e).WithField("channel", channel).WithField("data", string(data)).Errorln("parse event failure")
		return
	}

	instanceName := event.Args.Get("instance-name")
	if len(instanceName) > 0 {
		if instanceName != p.consumerConf.InstanceName {
			return
		}
	}

	switch event.Name {
	case "REBALANCE":
		{
			p.initQueues()
		}
	default:
		{
			logrus.WithField("channel", channel).WithField("data", string(data)).Warn("unknown event")
			return
		}
	}

	return
}
