package main

import (
	"encoding/csv"
	"github.com/WachirapatMT/electrack/cmd/internal"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"path"
	"sync"
)

var dataPath = "data"

func stream(wg *sync.WaitGroup, logger *logrus.Entry, file *os.File, channel chan internal.Message) {
	defer wg.Done()

	lines, err := csv.NewReader(file).ReadAll()
	if err != nil {
		logger.WithError(err).Error("cannot read csv file")
		return
	}

	logger.Info("start streaming message from csv file")
	for _, line := range lines {
		message := internal.Message{}
		err := message.FromCSVLine(line)
		if err != nil {
			logger.WithError(err).Warn("cannot parse message")
			continue
		}
		channel <- message
	}

	err = file.Close()
	if err != nil {
		logger.WithError(err).Warn("cannot close csv file")
	}
}

func main() {
	files, err := ioutil.ReadDir(dataPath)
	if err != nil {
		logrus.WithError(err).Error("cannot read directory")
		return
	}

	wg := &sync.WaitGroup{}
	for index, fileInfo := range files {
		logger := logrus.WithField("file", fileInfo.Name())
		logger.Info("file detected")

		// init sensor
		channel := make(chan internal.Message)
		producer, err := internal.NewProducer()
		if err != nil {
			logger.WithError(err).Error("cannot create new producer")
			continue
		}
		sensor := Sensor{
			MessageChannel: channel,
			SyncProducer:   &producer,
		}
		go sensor.Start(logger, int32(index))

		// stream data
		file, err := os.Open(path.Join(dataPath, fileInfo.Name()))
		if err != nil {
			logger.WithError(err).Error("cannot open csv file")
			continue
		}
		logger.Info("open file successfully")

		wg.Add(1)
		go stream(wg, logger, file, channel)
	}

	wg.Wait()
}
