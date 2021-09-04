package internal

import (
	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

type Sensor struct {
	MessageChannel chan Message

	SyncProducer *sarama.SyncProducer
}

func (s *Sensor) Start(logger *logrus.Entry, sensorID int32) {
	logger.Info("sensor started")
	for message := range s.MessageChannel {
		kafkaMessage, err := message.ToProducerMessage(sensorID)
		if err != nil {
			logger.WithError(err).Warn("cannot serialize message")
			continue
		}
		_, _, err = (*s.SyncProducer).SendMessage(kafkaMessage)
		if err != nil {
			logger.WithError(err).Warn("cannot producer message to kafka")
		}
	}
}