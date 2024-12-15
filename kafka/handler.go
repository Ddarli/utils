package kafka

import (
	"github.com/IBM/sarama"
)

type EventsHandler struct {
	eventsKafka chan *sarama.ConsumerMessage
}

func NewDefaultHandler(
	eventsKafka chan *sarama.ConsumerMessage,
) *EventsHandler {
	return &EventsHandler{eventsKafka: eventsKafka}
}

func (kc *EventsHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (kc *EventsHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	session.Commit()
	////wtfffff
	return nil
}

func (kc *EventsHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) (err error) {
	for message := range claim.Messages() {
		kc.eventsKafka <- message

		session.MarkMessage(message, "")
	}

	return err
}
