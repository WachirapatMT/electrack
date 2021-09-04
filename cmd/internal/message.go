package internal

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"strings"
)

const KafkaTopic = "electrack"

type Message struct {
	TimeStamp string `json:"time_stamp"`
	Value     string `json:"value"`
	Source    string `json:"source"`
}

func (m *Message) GetElasticIndexRequest() esapi.IndexRequest {
	return esapi.IndexRequest{
		Index: m.Source,
		DocumentID: m.TimeStamp,
		Body: strings.NewReader(m.Value),
		Refresh: "true",
	}
}

func (m *Message) ToProducerMessage(partition int32) (*sarama.ProducerMessage, error) {
	bytes, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}

	return &sarama.ProducerMessage{
		Topic:     KafkaTopic,
		Partition: partition,
		Value: sarama.StringEncoder(bytes),
	}, nil
}

func (m *Message) FromConsumerMessage(message *sarama.ConsumerMessage) error {
	return json.Unmarshal(message.Value, m)
}

func (m *Message) FromCSVLine(line []string) error {
	if len(line) != 3 {
		return errors.New(fmt.Sprintf("invalid csv format (%v)", line))
	}
	m.TimeStamp = line[0]
	m.Value = line[1]
	m.Source = line[2]
	return nil
}