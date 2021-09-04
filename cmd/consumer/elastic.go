package main

import (
	"github.com/WachirapatMT/electrack/cmd/internal"
	"github.com/sirupsen/logrus"
	"sync"
)

func elasticListener(wg *sync.WaitGroup, channel chan internal.Message)  {
	defer wg.Done()
	for message := range channel {
		logrus.Info("message received %v", message)
	}
}
