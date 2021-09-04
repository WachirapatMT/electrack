package internal

import (
	"github.com/Shopify/sarama"
	"os"
)

var (
	brokers = []string{os.Getenv("KAKFA_BROKER_ADDR")}
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
	return sarama.NewSyncProducer(brokers, config)
}

func NewConsumer() (sarama.Consumer, error) {
	return sarama.NewConsumer(brokers, config)
}
