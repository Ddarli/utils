package kafka

import (
	"context"
	"github.com/IBM/sarama"
	"github.com/dnwe/otelsarama"
	"go.opentelemetry.io/otel"
	"log"
)

type (
	ProducerConfig struct {
		RequireAcks int
		MaxAttempts int `validate:"required"`
		Compression int
		RetryMax    int
		Idempotent  struct {
			Mode            bool
			MaxOpenRequests int
			RetryMax        int
		}
	}
	AsyncProducer interface {
		SendMessage(ctx context.Context, message Message)
	}
	SyncProducer interface {
		SendMessage(ctx context.Context, message Message) (partition int32, offset int64, err error)
	}
	asyncProducer struct {
		sarama.AsyncProducer
	}
	syncProducer struct {
		sarama.SyncProducer
	}
)

func newAsyncProducer(brokers []string, cfg *sarama.Config) (*asyncProducer, error) {
	producer, err := sarama.NewAsyncProducer(brokers, cfg)
	if err != nil {
		return nil, err
	}

	producer = otelsarama.WrapAsyncProducer(cfg, producer)

	go func() {
		producer := producer
		for event := range producer.Errors() {
			log.Printf("failed to send message via kafka: %v", event.Err)
		}
	}()

	go func() {
		producer := producer
		for event := range producer.Successes() {
			log.Printf("successfully produced message: %v", event.Value)
		}
	}()

	return &asyncProducer{
		AsyncProducer: producer,
	}, nil
}

func (s *asyncProducer) SendMessage(ctx context.Context, message Message) {
	key := sarama.StringEncoder(message.Key)
	value := sarama.StringEncoder(message.Value)

	msg := &sarama.ProducerMessage{
		Topic:   message.Topic,
		Key:     key,
		Value:   value,
		Headers: message.Headers,
	}

	otel.GetTextMapPropagator().Inject(ctx, otelsarama.NewProducerMessageCarrier(msg))

	go func() {
		s.AsyncProducer.Input() <- msg
	}()
}

func newSyncProducer(brokers []string, cfg *sarama.Config) (*syncProducer, error) {
	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, err
	}

	producer = otelsarama.WrapSyncProducer(cfg, producer)

	return &syncProducer{
		SyncProducer: producer,
	}, nil
}

func (s *syncProducer) SendMessage(ctx context.Context, msg Message) (partition int32, offset int64, err error) {
	key := sarama.StringEncoder(msg.Key)
	value := sarama.StringEncoder(msg.Value)

	message := &sarama.ProducerMessage{
		Topic:   msg.Topic,
		Key:     key,
		Value:   value,
		Headers: msg.Headers,
	}

	otel.GetTextMapPropagator().Inject(ctx, otelsarama.NewProducerMessageCarrier(message))

	return s.SyncProducer.SendMessage(message)
}
