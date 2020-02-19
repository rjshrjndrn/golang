//  vim: set ts=4 sw=4 tw=0 foldmethod=indent noet :

package main

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	zookeeperConn = "11.2.1.15:2181"
	cgroup        = "Test1"
	topic1        = "sunbirddev.analytics_metrics"
	topic2        = "sunbirddev.pipeline_metrics"
)

type metrics struct {
	job_name  string
	partition int
	metrics   map[string]float64
}

func (m metrics) addMetrics {
	// Check job_name and partition is in metrics list
	switch index, ok := itemExists(m.job_name, m.partition); ok {
	case true:
		// add metrics to prometheusMetrics if index index
		for metric := range m.metrics {
			prometheusMetrics[index][m.metrics[metric]] += m.metrics[metric]
		}
		return
	// if not append the metrics in prometheusMetrics
	default:
		prometheusMetrics = append(prometheusMetrics, m)
	}
}

// Check specified job_name:partition pair exists in []prometheusMetrics
func itemExists(job_name string, partition int) (int, bool) {
	for index, value := range prometheusMetrics {
		if value["partition"] == partition {
			return index, true
		}
	}
	return 0, false
}

// Metrics value should be of value type float64
// else drop the value
func metricsValidator(m map[string]interface{}) map[string]float64 {
	var tempMap = make(map[string]float64)
	for key, val := range m {
		switch v, ok := val.(float64); ok {
		// converting interface to float64
		case true:
			tempMap[key] = v
		// Dropping not float64 values
		default:
			fmt.Println("Dropping non float64 value %v=%v", key, val)
		}
	}
	return tempMap
}

var prometheusMetrics []metrics

func main() {
	// setup sarama log to stdout
	sarama.Logger = log.New(os.Stdout, "", log.Ltime)
	// prometheus.MustRegister(gauge)
	http.HandleFunc("/metrics", serve)
	// init consumer
	cg, err := initConsumer()
	if err != nil {
		fmt.Println("Error consumer goup: ", err.Error())
		os.Exit(1)
	}
	defer cg.Close()
	// run consumer
	fmt.Println(consume)
	go consume(cg)
	log.Fatal(http.ListenAndServe(":8000", nil))
}

func serve(w http.ResponseWriter, r *http.Request) {
	for _, value := range prometheusMetrics {
		for k, j := range value.metrics {
			// tmp, _ := k.(string)
			fmt.Fprintf(w, "samza_metrics_%v{job_name=\"%v\",partition=\"%v\"} %v\n", strings.ReplaceAll(k, "-", "_"), value.job_name, value.partition, j)
		}
	}
}

func initConsumer() (*consumergroup.ConsumerGroup, error) {
	// consumer config
	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetOldest
	config.Offsets.ProcessingTimeout = 10 * time.Second
	// join to consumer group
	cg, err := consumergroup.JoinConsumerGroup(cgroup, []string{topic1, topic2}, []string{zookeeperConn}, config)
	if err != nil {
		return nil, err
	}
	return cg, err
}

// CleanUp json to metrics: value pair
// extract job_name and partition
func convertor(jsons []byte) {
	var m map[string]interface{}
	err := json.Unmarshal(jsons, &m)
	if err != nil {
		panic(err)
	}
	job_name, _ := m["job-name"].(string)
	partition, _ := m["partition"].(int)
	delete(m, "metricts")
	delete(m, "job-name")
	delete(m, "partition")
	metric := metrics{job_name, partition, metricsValidator(m)}
	metric.addMetrics()
}

func consume(cg *consumergroup.ConsumerGroup) {
	for {
		select {
		case message := <-cg.Messages():
			convertor(message.Value)
			// CleanUp slice
			message.Value = nil
			err := cg.CommitUpto(message)
			if err != nil {
				fmt.Println("Error commit zookeeper: ", err.Error())
			}
		}
	}
}
