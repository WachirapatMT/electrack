package main

import (
	"context"
	"github.com/WachirapatMT/electrack/cmd/internal"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/sirupsen/logrus"
	"sync"
)

var elasticHosts = []string{"localhost:9200", "localhost:9300"}


func messageHandler(ctx context.Context, client *elasticsearch.Client, message internal.Message) {
	res, err := message.GetElasticIndexRequest().Do(ctx, client)
	if err != nil {
		logrus.WithError(err).Error("cannot construct index request")
	}
	defer res.Body.Close()
}

func elasticListener(wg *sync.WaitGroup, channel chan internal.Message) {
	defer wg.Done()

	ctx := context.Background()

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
		messageHandler(ctx, client, message)
	}
}
