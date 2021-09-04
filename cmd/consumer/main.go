package main

import (
	"github.com/Shopify/sarama"
	"github.com/WachirapatMT/electrack/cmd/internal"
	"github.com/sirupsen/logrus"
	"sync"
)

var masterConsumer sarama.Consumer

func init() {
	consumer, err := internal.NewConsumer()
	if err != nil {
		logrus.WithError(err).Error("cannot create new master consumer")
		return
	}
	masterConsumer = consumer
}

func consume(wg *sync.WaitGroup, partition int32, channel chan internal.Message) {
	defer wg.Done()

	logger := logrus.WithField("partition", partition)
	partitionConsumer, err := masterConsumer.ConsumePartition(internal.KafkaTopic, partition, sarama.OffsetOldest)
	if err != nil {
		logger.WithError(err).Error("cannot create new partition consumer")
		return
	}
	for consumerMessage := range partitionConsumer.Messages() {
		message := internal.Message{}
		err := message.FromConsumerMessage(consumerMessage)
		if err != nil {
			logger.WithError(err).Warn("cannot parse message")
			continue
		}

		channel <- message
	}
}

func main() {
	partitions, err := masterConsumer.Partitions(internal.KafkaTopic)
	if err != nil {
		logrus.WithError(err).Error("cannot get partitions")
		return
	}

	wg := &sync.WaitGroup{}
	channel := make(chan internal.Message)
	for _, partition := range partitions {
		wg.Add(1)
		go consume(wg, partition, channel)
	}

	wg.Add(1)
	go elasticListener(wg, channel)

	wg.Wait()
}