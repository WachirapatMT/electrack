package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"regexp"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

type Data struct {
	TimeStamp string
	Value     string
	Source    string
}

var brokers = []string{"127.0.0.1:9092"}
var w sync.WaitGroup

func newProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewManualPartitioner
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(brokers, config)

	return producer, err
}

func prepareMessage(topic, message string, partition int) *sarama.ProducerMessage {
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: int32(partition),
		Value:     sarama.StringEncoder(message),
	}

	return msg
}

func prepareData() map[int][]Data {
	var allRecord = make(map[int][]Data)

	var fileNameList = []string{
		"apparentTemperature.csv",
		"Barn [kW].csv",
		"cloudCover.csv",
		"dewPoint.csv",
		"Dishwasher [kW].csv",
		"Fridge [kW].csv",
		"Furnace 1 [kW].csv",
		"Furnace 2 [kW].csv",
		"Garage door [kW].csv",
		"Home office [kW].csv",
		"humidity.csv",
		"icon.csv",
		"Kitchen 12 [kW].csv",
		"Kitchen 14 [kW].csv",
		"Kitchen 38 [kW].csv",
		"Living room [kW].csv",
		"Microwave [kW].csv",
		"precipIntensity.csv",
		"precipProbability.csv",
		"pressure.csv",
		"Solar [kW].csv",
		"summary.csv",
		"temperature.csv",
		"visibility.csv",
		"Well [kW].csv",
		"windBearing.csv",
		"windSpeed.csv",
		"Wine cellar [kW].csv",
	}
	for index, fileName := range fileNameList {
		fmt.Println("Process file :", fileName)
		path := ""

		allRecord[index] = readCsvFile(path + fileName)
		fmt.Println("Finish Process file: ", fileName, " all records : ", len(allRecord[index]))
	}

	return allRecord
}

func readCsvFile(filePath string) []Data {
	var records []Data
	var intCheck = regexp.MustCompile(`^[0-9]+$`)
	if _, err := os.Stat(filePath); err == nil {
		f, err := os.Open(filePath)
		if err != nil {
			log.Fatal("Unable to read input file "+filePath, err)
		}
		defer f.Close()
		csvLines, err := csv.NewReader(f).ReadAll()
		if err != nil {
			log.Fatal("Unable to parse file as CSV for "+filePath, err)
		}
		for _, line := range csvLines {
			if intCheck.MatchString(line[0]) {
				record := Data{TimeStamp: line[0], Value: line[1], Source: line[2]}
				records = append(records, record)
			}
		}

	} else if os.IsNotExist(err) {
		fmt.Println(filePath, " : not exist in this folder")
	}

	return records
}

func startAllProducer() map[int]sarama.SyncProducer {
	allProducer := make(map[int]sarama.SyncProducer)
	for i := 0; i < 28; i++ {
		if producer, err := newProducer(); err == nil {
			allProducer[i] = producer
		} else {

			log.Fatal("Unable to create Producer ", err)
		}
	}

	return allProducer
}

func sendsignalAll(sensors map[int]sarama.SyncProducer, data map[int][]Data, time int) {
	for i := 0; i < time; i++ {
		for index, sensor := range sensors {
			if byteMsg, err := json.Marshal(data[index][i]); err == nil {
				msg := prepareMessage("electrack", string(byteMsg), index)
				partition, _, err := sensor.SendMessage(msg)
				if err != nil {
					log.Fatal("Can't send message to", index, " Partition :", err)
				}
				if int(partition) != index {
					log.Fatal("Send to wrong partition from ", index, " partition to ", partition)
				}
			}

		}
		if i%1000 == 0 {
			fmt.Println("current send message : ", i)
		}
	}

}
func sendsignal(sensor sarama.SyncProducer, data []Data, time int, sensorNumber int) {
	for i := 0; i < time; i++ {

		if byteMsg, err := json.Marshal(data[i]); err == nil {
			msg := prepareMessage("electrack", string(byteMsg), sensorNumber)
			partition, _, err := sensor.SendMessage(msg)
			if err != nil {
				log.Fatal("Can't send message to", sensorNumber, " Partition :", err)
			}
			if int(partition) != sensorNumber {
				log.Fatal("Send to wrong partition from ", sensorNumber, " partition to ", partition)
			}
		}

		if i%1000 == 0 {
			fmt.Println("current send message : ", i, " from sensor ", data[0].Source)
		}
	}
	defer w.Done()

}

func main() {
	AllData := prepareData()
	fmt.Println("Total file : ", len(AllData))
	// create all producer
	time.Sleep(3 * time.Second)
	sensors := startAllProducer()

	// sendsignalAll
	fileCount := (len(AllData))
	fmt.Println("start sending all data")
	w.Add(fileCount)
	for i := 0; i < fileCount; i++ {
		go sendsignal(sensors[i], AllData[i], len(AllData[i]), i)
	}

	w.Wait()
	fmt.Println("finish sending all data")

}
