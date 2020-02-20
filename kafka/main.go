package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

type metrics struct {
	Ets   int64
	Value string
	Name  string
}

// Create a new metrics instance with
// default values except measurement
func NewMetric(val string) ([]byte, error) {
	metric := metrics{}
	metric.Value = val
	metric.Ets = time.Now().UnixNano() / 1000000 // Converting nano to milliseconds
	metric.Name = "Tds Metrics"
	return json.Marshal(metric)
}

func helloError(res http.ResponseWriter, r *http.Request) {
	res.WriteHeader(500)
	res.Write([]byte("Boom!"))
}

func helloWorld(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		reqBody, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Fatal(err)
		}
		// Creating json metrics for kafka
		newMetric, err := NewMetric(string(reqBody))
		if err != nil {
			log.Printf("Json serialization failed for data %s; error is %q", string(reqBody), err)
		}
		fmt.Printf("%s\n", newMetric)
		// If got post, put it into kakfa
		go func(payLoad []byte) {
			kafka_writer.WriteMessages(context.Background(),
				kafka.Message{
					Value: payLoad,
				})
			if err != nil {
				log.Fatal(err)
			}
		}(newMetric)
		fmt.Fprintf(w, "Body: %s\n", reqBody)
	default:
		for k, v := range r.Header {
			fmt.Fprintf(w, "%s: %s\n", k, v)
		}
	}
	fmt.Fprintf(w, "HTTPVersion: %s\n", r.Proto)
	fmt.Fprintf(w, "RequestPath: %s\n", r.URL.Path)
	fmt.Fprintf(w, "Version: v3")
}

var kafka_writer *kafka.Writer

func main() {
	fmt.Printf("Initializing Kafka connection\n")
	// Getting kafka host
	// kafka_host := "50.1.0.5:9092" // os.Getenv("kafka_host")
	kafka_host := os.Getenv("kafka_host")
	// Getting kafka topic
	// kafka_topic := "devcon.iot.metrics" //os.Getenv("kafka_topic")
	kafka_topic := os.Getenv("kafka_topic")
	// Checking for mandatory variables
	if kafka_topic == "" || kafka_host == "" {
		log.Fatalf("kafka_topic or kafka_host environment variables not set")
	}
	// make a writer that produces to topic-A, using the least-bytes distribution
	kafka_writer = kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafka_host},
		Topic:    kafka_topic,
		Balancer: &kafka.LeastBytes{},
	})
	fmt.Printf("Kafka connection initialized\nhost: %s\ntopic: %s\n", kafka_host, kafka_topic)

	http.HandleFunc("/", helloWorld)
	http.HandleFunc("/error", helloError)
	fmt.Println("HTTP server started on :4000")
	if err := http.ListenAndServe(":4000", nil); err != nil {
		log.Fatal(err)
	}
}
