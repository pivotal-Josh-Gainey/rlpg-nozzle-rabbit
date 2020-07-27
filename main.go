package main

import (
	"code.cloudfoundry.org/go-loggregator"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"context"
	"github.com/golang/protobuf/jsonpb"
	"github.com/streadway/amqp"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"
)

func main() {

	http.HandleFunc("/health", healthChecker)

	//get rabbit ready
	rabbitURL := os.Getenv("RABBITURL")
	if rabbitURL == "" {
		log.Fatalf("RABBITURL is required")
	}
	connection, err := amqp.Dial(rabbitURL)
	failOnError(err, "Failed to connect to RabbitMQ")
	channel, err := connection.Channel()
	failOnError(err, "Failed to open a channel")
	queue, err := channel.QueueDeclare(
		"speclogger", // name
		false,        // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare a queue")

	//init server
	go func() {
		log.Fatal(http.ListenAndServe(":8080", nil))
	}()

	//connect
	rlpAddr := os.Getenv("LOG_STREAM_ADDR")
	if rlpAddr == "" {
		log.Fatal("LOG_STREAM_ADDR is required")
	}

	token := os.Getenv("TOKEN")
	if token == "" {
		log.Fatalf("TOKEN is required")
	}

	c := loggregator.NewRLPGatewayClient(
		rlpAddr,
		loggregator.WithRLPGatewayClientLogger(log.New(os.Stderr, "", log.LstdFlags)),
		loggregator.WithRLPGatewayHTTPClient(&tokenAttacher{
			token: token,
		}),
	)

	//select app
	sourceID := os.Getenv("SOURCEID")
	if sourceID == "" {
		log.Fatalf("sourceID is required")
	}

	var selectors = []*loggregator_v2.Selector{
		{SourceId: sourceID, Message: &loggregator_v2.Selector_Log{Log: &loggregator_v2.LogSelector{}}},
	}

	//get stream
	shardID := os.Getenv("SHARDID")
	if shardID == "" {
		log.Fatalf("shardID is required")
	}
	es := c.Stream(context.Background(), &loggregator_v2.EgressBatchRequest{
		ShardId: shardID,
		Selectors: selectors,
	})
	marshaler := jsonpb.Marshaler{}
	for {
		for _, e := range es() {
			if err := marshaler.Marshal(ioutil.Discard, e); err != nil {
				log.Fatal(err)
			}else{
				publish := channel.Publish(
					"",     // exchange
					queue.Name, // routing key
					false,  // mandatory
					false,  // immediate
					amqp.Publishing{
						ContentType: "text/plain",
						Body:        []byte(e.GetLog().String()),
					})
				failOnError(publish, "Failed to publish a message")
			}
		}
	}
}

func healthChecker(w http.ResponseWriter, r *http.Request) {
	//logic for the healthcheck endpoint
	rand.Seed(makeTimestamp())
	num := rand.Intn(5 - 0) + 0
	if num == 4{
		//return not 200
		w.WriteHeader(404)
	}else{
		//return 200
		w.WriteHeader(200)
	}
}

type tokenAttacher struct {
	token string
}

func (a *tokenAttacher) Do(req *http.Request) (*http.Response, error) {
	req.Header.Set("Authorization", a.token)
	return http.DefaultClient.Do(req)
}

func makeTimestamp() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
