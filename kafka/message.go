package kafka

import "github.com/IBM/sarama"

type (
	Message struct {
		Topic   string
		Key     []byte
		Value   []byte
		Headers []sarama.RecordHeader
	}
)
