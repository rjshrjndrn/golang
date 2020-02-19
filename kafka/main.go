package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

func helloError(res http.ResponseWriter, r *http.Request) {
	res.WriteHeader(500)
	res.Write([]byte("Boom!"))
}
func kafkaObject() *kafka.Conn {
	// Getting kafka host
	kafka_host := "50.1.0.5:9092" // os.Getenv("kafka_host")
	// Getting kafka topic
	kafka_topic := "devcon.iot.metrics" //os.Getenv("kafka_topic")
	// Checking for mandatory variables
	if kafka_topic == "" || kafka_host == "" {
		log.Panic("kafka_topic or kafka_host environment variables not set")
	}

	// Creating Kafka connection
	conn, err := kafka.DialLeader(context.Background(), "tcp", kafka_host, kafka_topic, 0)
	if err != nil {
		log.Fatalf("Coudn't connect to kafka. error is %q", err)
	}
	return conn
}

type metrics struct {
	Ets   int64
	Value string
	Name  string
}

var conn *kafka.Conn

// Create a new metrics instance with
// default values except measurement
func NewMetric(val string) ([]byte, error) {
	metric := metrics{}
	metric.Value = val
	metric.Ets = time.Now().UnixNano() / 1000000 // Converting nano to milliseconds
	metric.Name = "Tds Metrics"
	return json.Marshal(metric)
}

func helloWorld(w http.ResponseWriter, r *http.Request) {

	switch r.Method {
	case "POST":
		// defer conn.Close()
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
		go func(reqBody []byte) {
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			_, err := conn.WriteMessages(
				kafka.Message{Value: reqBody},
			)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("Conn address: %s\n", &conn)
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

func main() {
	fmt.Printf("Initializing Kafka")
	conn = kafkaObject()
	http.HandleFunc("/", helloWorld)
	http.HandleFunc("/error", helloError)
	if err := http.ListenAndServe(":4000", nil); err != nil {
		log.Fatal(err)
	}
}
