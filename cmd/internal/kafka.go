package internal

import "github.com/Shopify/sarama"

var (
	brokers = []string{"127.0.0.1:9092"}
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
