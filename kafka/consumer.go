package kafka

import (
	"context"
	"errors"
	"github.com/IBM/sarama"
	"github.com/dnwe/otelsarama"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"log"
)

type (
	ConsumerGroup interface {
		Consume(ctx context.Context)
		Cleanup(ctx context.Context)
	}
	ConsumerConfig struct {
		Addresses    []string `validate:"required,min=1"`
		Assignor     string
		OffsetNewest bool
		AutoCommit   bool
	}

	kafkaConsumerGroup struct {
		topics        []string
		consumers     sarama.ConsumerGroup
		eventsHandler sarama.ConsumerGroupHandler
		tracer        trace.Tracer
	}
)

func newConsumerGroup(
	brokers []string,
	groupID string,
	topics []string,
	cfg *sarama.Config,
	eventsHandler sarama.ConsumerGroupHandler,
) (consumer *kafkaConsumerGroup) {
	tracer := otel.GetTracerProvider().Tracer("kafkaconnector")

	eventsHandler = otelsarama.WrapConsumerGroupHandler(eventsHandler)

	consumers, err := sarama.NewConsumerGroup(brokers, groupID, cfg)
	if err != nil {
		log.Fatalf("Error creating consumer group: %v", err)
	}

	return &kafkaConsumerGroup{
		topics:        topics,
		consumers:     consumers,
		eventsHandler: eventsHandler,
		tracer:        tracer,
	}
}

func (k *kafkaConsumerGroup) Consume(ctx context.Context) {
	for {
		err := k.consumers.Consume(ctx, k.topics, k.eventsHandler)

		if errors.Is(err, sarama.ErrClosedConsumerGroup) {
			return
		}
		if err != nil {
			log.Printf("Error starting consuming session: %v\n", err)
		}
	}
}

func (k *kafkaConsumerGroup) Cleanup(_ context.Context) {
	err := k.consumers.Close()
	if err != nil {
		log.Printf("error closing consumer: %s", err.Error())
	}
}
