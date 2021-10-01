package internal

import (
	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
	"os"
)

var (
	brokers = []string{os.Getenv("KAFKA_BROKER_ADDR")}
	config  *sarama.Config
)

func init() {
	config = sarama.NewConfig()

	// producer config
	config.Producer.Partitioner = sarama.NewManualPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
}

func NewProducer() (sarama.SyncProducer, error) {
	logrus.Infof("new sync producer with %v", brokers)
	return sarama.NewSyncProducer(brokers, config)
}

func NewConsumer() (sarama.Consumer, error) {
	return sarama.NewConsumer(brokers, config)
}
