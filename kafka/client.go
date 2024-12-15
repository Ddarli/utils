package kafka

import (
	"github.com/IBM/sarama"
	"log"
	"time"
)

type (
	Client interface {
		NewConsumerGroup(groupID string, topics []string, eventsHandler sarama.ConsumerGroupHandler) ConsumerGroup
		NewSyncProducer() SyncProducer
		NewAsyncProducer() AsyncProducer
	}

	ClientConfig struct {
		Brokers            []string `validate:"required"`
		InsecureSkipVerify bool
		Producer           *ProducerConfig
		Consumer           *ConsumerConfig
	}

	kafkaClient struct {
		sarama.Client
		brokers []string
	}
)

func NewClient(cfg ClientConfig) *kafkaClient {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0

	if cfg.Producer != nil {
		config.Producer.Retry.Max = cfg.Producer.RetryMax
		config.Producer.Retry.Backoff = time.Millisecond * 250
		config.Producer.Idempotent = cfg.Producer.Idempotent.Mode

		if cfg.Producer.Idempotent.Mode {
			config.Producer.Retry.Max = cfg.Producer.Idempotent.RetryMax
			config.Net.MaxOpenRequests = cfg.Producer.Idempotent.MaxOpenRequests
			config.Producer.RequiredAcks = sarama.RequiredAcks(cfg.Producer.RequireAcks)
		}

		config.Producer.Return.Successes = true
		config.Producer.Return.Errors = true
	}

	if cfg.Consumer != nil {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
		if cfg.Consumer.OffsetNewest {
			config.Consumer.Offsets.Initial = sarama.OffsetNewest
		}

		switch cfg.Consumer.Assignor {
		case "sticky":
			config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()}
		case "round-robin":
			config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
		case "range":
			config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
		default:
			log.Panicf("Unrecognized collector group partition assignor: %s", cfg.Consumer.Assignor)
		}

		config.Consumer.Return.Errors = true

		config.Consumer.Offsets.AutoCommit.Enable = cfg.Consumer.AutoCommit
	}

	saramaCli, err := sarama.NewClient(cfg.Brokers, config)
	if err != nil {
		log.Fatalf("failed to create kafka client: %e", err)
	}

	return &kafkaClient{
		brokers: cfg.Brokers,
		Client:  saramaCli,
	}
}

func (k kafkaClient) NewConsumerGroup(groupID string, topics []string, eventsHandler sarama.ConsumerGroupHandler) ConsumerGroup {
	return newConsumerGroup(k.brokers, groupID, topics, k.Config(), eventsHandler)
}

func (k kafkaClient) NewSyncProducer() SyncProducer {
	producer, err := newSyncProducer(k.brokers, k.Config())
	if err != nil {
		log.Fatalf("failed to crete sync producer: %s", err.Error())
	}

	return producer
}

func (k kafkaClient) NewAsyncProducer() AsyncProducer {
	producer, err := newAsyncProducer(k.brokers, k.Config())
	if err != nil {
		log.Fatalf("failed to crete sync producer: %s", err.Error())
	}

	return producer
}
