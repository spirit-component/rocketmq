package redis

import (
	"fmt"
	"github.com/pkg/errors"
	"strconv"
	"strings"
	"sync"
	"time"

	rmq "github.com/apache/rocketmq-client-go/core"
	"github.com/gogap/config"
	"github.com/gomodule/redigo/redis"
	"github.com/sirupsen/logrus"
	"github.com/spirit-component/rocketmq/queue_table"
)

type RedisConfig struct {
	Address     string
	Db          int
	Password    string
	MaxIdle     int
	IdleTimeout time.Duration
	KeyPrefix   string
}

type RedisQueueTable struct {
	redisConfig  RedisConfig
	pullConsumer rmq.PullConsumer
	consumerConf *rmq.PullConsumerConfig

	topic          string
	expression     string
	queueTableConf config.Configuration

	queues       []rmq.MessageQueue
	queueOffsets map[int]*int64

	redisPool *redis.Pool

	queueLocker sync.RWMutex

	eventChannel  string
	pubSubChannel *redis.PubSubConn
}

func init() {
	queue_table.RegisterQueueTable("redis", NewRedisQueueTable)
}

func NewRedisQueueTable(consumer rmq.PullConsumer, topic, expr string, consumerConf *rmq.PullConsumerConfig, queueTableConf config.Configuration) (table queue_table.QueueTable, err error) {

	if len(consumerConf.InstanceName) == 0 {
		err = fmt.Errorf("consumer config of InstanceName is empty")
		return
	}

	redisConf := RedisConfig{
		Address:     queueTableConf.GetString("address", "127.0.0.1:6379"),
		Db:          int(queueTableConf.GetInt32("db", 1)),
		Password:    queueTableConf.GetString("password", ""),
		MaxIdle:     int(queueTableConf.GetInt32("max-idel", 2)),
		IdleTimeout: queueTableConf.GetTimeDuration("idle-timeout", time.Second*30),
		KeyPrefix:   queueTableConf.GetString("key-prefix", "rmq:qt"),
	}

	qt := &RedisQueueTable{
		pullConsumer:   consumer,
		consumerConf:   consumerConf,
		topic:          topic,
		expression:     expr,
		queueTableConf: queueTableConf,
		queueOffsets:   make(map[int]*int64),
		redisConfig:    redisConf,
		eventChannel: strings.Join([]string{
			redisConf.KeyPrefix,
			topic,
			"event",
		}, ":"),
	}

	redisPool := &redis.Pool{
		MaxIdle:      redisConf.MaxIdle,
		IdleTimeout:  redisConf.IdleTimeout,
		TestOnBorrow: qt.testOnBorrow,
		Dial:         qt.redisDailer,
	}

	qt.redisPool = redisPool

	return qt, nil
}

func (p *RedisQueueTable) Start() (err error) {
	err = p.initQueues()
	if err != nil {
		return
	}

	err = p.startEventListen()
	if err != nil {
		return
	}

	return
}

func (p *RedisQueueTable) Stop() (err error) {

	logrus.WithFields(logrus.Fields{
		"topic":      p.topic,
		"expression": p.expression,
		"provider":   "redis",
	}).Info("stopping redis event listen")

	err = p.pubSubChannel.Unsubscribe()
	if err != nil {
		return
	}

	err = p.redisPool.Close()
	if err != nil {
		return
	}

	return
}

func (p *RedisQueueTable) Queues() (queues []rmq.MessageQueue) {
	p.queueLocker.RLock()
	queues = p.queues
	p.queueLocker.RUnlock()

	return queues
}

func (p *RedisQueueTable) CurrentOffset(broker string, queueID int) (ret int64, err error) {

	conn := p.redisPool.Get()
	defer conn.Close()

	key := strings.Join([]string{
		p.redisConfig.KeyPrefix,
		p.topic,
		p.expression,
		p.consumerConf.InstanceName,
		broker,
		"queue",
		strconv.Itoa(queueID),
	}, ":")

	offset, err := redis.Int64(conn.Do("GET", redis.Args{}.Add(key)...))
	if err != nil {
		err = errors.WithMessagef(err, "get queue offset failure, key: %s", key)
		return
	}

	ret = offset

	return
}

func (p *RedisQueueTable) UpdateOffset(broker string, queueID int, nextBeginOffset int64) (err error) {

	conn := p.redisPool.Get()
	defer conn.Close()

	key := strings.Join([]string{
		p.redisConfig.KeyPrefix,
		p.topic,
		p.expression,
		p.consumerConf.InstanceName,
		broker,
		"queue",
		strconv.Itoa(queueID),
	}, ":")

	_, err = conn.Do("SET", redis.Args{}.Add(key, nextBeginOffset)...)
	if err != nil {
		err = errors.WithMessagef(err, "set queue offset failure, key: %s, value: %d", key, nextBeginOffset)
		return
	}

	return
}

func (p *RedisQueueTable) initQueues() (err error) {

	p.queueLocker.Lock()
	defer p.queueLocker.Unlock()

	queues := p.pullConsumer.FetchSubscriptionMessageQueues(p.topic)

	logrus.WithFields(
		logrus.Fields{
			"topic":       p.topic,
			"queue-count": len(queues),
			"expression":  p.expression,
			"provider":    "redis",
		},
	).Debugln("queues fetched")

	brokers := map[string]bool{}

	for _, q := range queues {
		brokers[q.Broker] = true
	}

	var subQueues []rmq.MessageQueue
	for broker := range brokers {

		key := strings.Join([]string{
			p.redisConfig.KeyPrefix,
			p.topic,
			p.consumerConf.InstanceName,
			broker,
			"queues",
		}, ":")

		conn := p.redisPool.Get()
		defer conn.Close()

		var queueIDs []int
		queueIDs, err = redis.Ints(conn.Do("SMEMBERS", key))
		if err != nil {
			err = errors.WithMessagef(err, "get queues failure, key: %s", key)
			return
		}

		if len(queueIDs) == 0 {
			logrus.WithFields(
				logrus.Fields{"broker": broker,
					"queues": queueIDs,
					"topic":  p.topic,
					"key":    key,
				}).Warnln("queue list is empty")
		} else {
			logrus.WithFields(
				logrus.Fields{"broker": broker,
					"queues": queueIDs,
					"topic":  p.topic,
					"key":    key,
				}).Debug("queues subscribed")
		}

		mapQueueIDs := map[int]bool{}
		for _, id := range queueIDs {
			mapQueueIDs[int(id)] = true
		}

		for i := 0; i < len(queues); i++ {
			if mapQueueIDs[queues[i].ID] {
				err = p.initQueueOffset(queues[i])
				if err != nil {
					return
				}
				subQueues = append(subQueues, queues[i])
			}
		}
	}

	p.queues = subQueues

	return
}

func (p *RedisQueueTable) initQueueOffset(mq rmq.MessageQueue) (err error) {

	conn := p.redisPool.Get()
	defer conn.Close()

	key := strings.Join([]string{
		p.redisConfig.KeyPrefix,
		p.topic,
		p.expression,
		p.consumerConf.InstanceName,
		mq.Broker,
		"queue",
		strconv.Itoa(mq.ID),
	}, ":")

	exists, err := redis.Bool(conn.Do("EXISTS", key))
	if err != nil {
		err = errors.WithMessagef(err, "get queue offset failure, key: %s", key)
		return
	}

	if !exists {
		pullResult := p.pullConsumer.Pull(mq, p.expression, 0, 1)

		_, err = conn.Do("SETNX", redis.Args{}.Add(key, pullResult.MaxOffset)...)
		if err != nil {
			err = errors.WithMessagef(err, "set queue offset failure, key: %s, value: %d", key, pullResult.MaxOffset)
			return
		}

		logrus.WithFields(
			logrus.Fields{
				"topic":    p.topic,
				"queue-id": mq.ID,
				"offset":   pullResult.MaxOffset,
			},
		).Debugln("init queue offset")
	}

	return
}

func (p *RedisQueueTable) redisDailer() (conn redis.Conn, err error) {
	c, err := redis.Dial("tcp", p.redisConfig.Address)
	if err != nil {
		return
	}

	if len(p.redisConfig.Password) > 0 {
		_, err = c.Do("AUTH", p.redisConfig.Password)
		if err != nil {
			return
		}
	}

	_, err = c.Do("SELECT", p.redisConfig.Db)
	if err != nil {
		return
	}

	_, err = c.Do("PING")
	if err != nil {
		return
	}

	conn = c
	return
}

func (p *RedisQueueTable) testOnBorrow(c redis.Conn, t time.Time) error {
	if time.Since(t) < time.Minute {
		return nil
	}
	_, err := c.Do("PING")
	return err
}
