package app

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/IBM/sarama"
)

func ConsumeKafkaMessage(ctx context.Context, topic string) (string, error) {
	log.Printf("Inside ConsumeKafkaMessage to consume message")

	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, nil)
	if err != nil {
		return "", err
	}
	defer consumer.Close()

	partitions, err := consumer.Partitions(topic)
	if err != nil {
		return "", err
	}

	if len(partitions) == 0 {
		return "", nil
	}
	partitionConsumer, err := consumer.ConsumePartition(topic, partitions[0], sarama.OffsetNewest)
	if err != nil {
		return "", err
	}
	defer partitionConsumer.Close()

	log.Printf("Waiting for a message on topic %s", topic)

	select {
	case msg := <-partitionConsumer.Messages():
		log.Printf("Consumed message is %v", string(msg.Value))
		return string(msg.Value), nil
	case <-time.After(20 * time.Second):
		log.Print("Timeout...No messages consumed")
		return "", nil
	case <-ctx.Done():
		log.Printf("Context done, stopping consumer")
		return "", nil
	}
}

func FetchData(ctx context.Context, kakfaMessage string) (ProductData, error) {
	log.Printf("Inside FetchData to make API call To get device details")
	var objectDetails ApiResponse
	var responseObject ProductData

	randomInt := rand.Intn(9) + 1
	url := fmt.Sprintf("https://api.restful-api.dev/objects/%d", randomInt)

	resp, err := http.Get(url)

	if err != nil {
		return ProductData{}, err
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(&objectDetails); err != nil {
		return ProductData{}, err
	}

	id, err := strconv.Atoi(objectDetails.ID)
	if err != nil {
		return ProductData{}, err // Return an error if conversion fails
	}
	log.Print("Data Fetched Successfully")

	responseObject.Id = id
	responseObject.Name = objectDetails.Name
	responseObject.Color = objectDetails.Data.Color

	log.Print("Appending the Kafka Message to the responseObject")
	responseObject.AdditionalData.KafkaMessage = kakfaMessage

	log.Printf("Device No %v , %v is of Color %v", responseObject.Id, responseObject.Name, responseObject.Color)

	return responseObject, nil
}

func PublishKafkaMessage(topic string, data ProductData) (string, error) {

	log.Printf("Inside PublishKafkaMessage to make Publish Messageto Kafka Topic")
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true // Enables success response from the producer

	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("Error creating producer: %v", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalf("Error closing producer: %v", err)
		}
	}()

	publishMessage, err := messageBuilder(data)
	if err != nil {
		log.Fatalf("Error building message %v", err)

	}

	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(publishMessage),
	}

	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		log.Fatalf("Error sending message: %v", err)
		return "", err
	}
	fmt.Printf("The published message is %v", publishMessage)
	fmt.Printf("Message sent to partition %d at offset %d\n", partition, offset)
	return publishMessage, err
}

func messageBuilder(p ProductData) (string, error) {
	message, err := json.Marshal(p)
	if err != nil {
		return "", err
	}

	return string(message), nil
}

// @@@SNIPEND
