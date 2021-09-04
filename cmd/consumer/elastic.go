package main

import (
	"github.com/WachirapatMT/electrack/cmd/internal"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/sirupsen/logrus"
	"sync"
)

func elasticListener(wg *sync.WaitGroup, channel chan internal.Message) {
	_, err := elasticsearch.NewDefaultClient()
	if err != nil {
		logrus.WithError(err).Error("cannot create default elastic client")
		return
	}
	defer wg.Done()
	for message := range channel {
		logrus.Info("message received %v", message)
	}
}
