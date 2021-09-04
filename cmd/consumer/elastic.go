package main

import (
	"context"
	"encoding/json"
	"github.com/WachirapatMT/electrack/cmd/internal"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/sirupsen/logrus"
	"sync"
)

var elasticHosts = []string{"http://localhost:9200", "http://localhost:9300"}

type ElasticResponse = map[string]interface{}

func messageHandler(client *elasticsearch.Client, message internal.Message) {
	res, err := message.GetElasticIndexRequest().Do(context.Background(), client)
	if err != nil {
		logrus.WithError(err).Error("cannot send http request to elastic")
		return
	}
	defer res.Body.Close()

	if res.IsError() {
		elasticResponse := ElasticResponse{}
		err := json.NewDecoder(res.Body).Decode(&elasticResponse)
		if err != nil {
			logrus.WithField("body", "unknown").Error("request error when push document to elastic search")
		} else {
			logrus.WithField("body", elasticResponse).Error("request error when push document to elastic search")
		}
		return
	}
}

func elasticListener(wg *sync.WaitGroup, channel chan internal.Message) {
	defer wg.Done()

	client, err := elasticsearch.NewClient(elasticsearch.Config {
		Addresses: elasticHosts,
	})
	if err != nil {
		logrus.WithError(err).Error("cannot create default elastic client")
		return
	}

	_, err = client.Info()
	if err != nil {
		logrus.WithError(err).Error("cannot establish request to elastic")
	}

	for message := range channel {
		logrus.Info("message received")
		messageHandler(client, message)
	}
}
