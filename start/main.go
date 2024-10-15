package main

import (
	"context"
	"log"

	"go.temporal.io/sdk/client"

	"money-transfer-project-template-go/app"
)

// WHAT ARE WE GOING TO WORK ON
// 1-> CREATE A WORKFLOW "GET DEVICE DETAILS"
// 2-> THE WORKFLOW HAS 3 ACTIVITY INSIDE
//  2.1->FETCH DEVICE DETAILS FROM API
//  2.2->PUBLISH THE DEVICE DATA TO KAFKA TOPIC
//  2.3->CONSUME DEVICE DETAILS FROM KAFKA

// @@@SNIPSTART money-transfer-project-template-go-start-workflow
func main() {
	// Create the client object just once per process
	c, err := client.Dial(client.Options{})

	if err != nil {
		log.Fatalln("Unable to create Temporal client:", err)
	}

	defer c.Close()

	options := client.StartWorkflowOptions{
		ID:        "kafka-workflow-001",
		TaskQueue: app.KafkaWorkflowTaskQueueName,
	}

	log.Printf("Starting Kafka Workflow to consume message, make a API call and Publish the message to kafka topic")

	we, err := c.ExecuteWorkflow(context.Background(), options, app.KafkaWorkflow)
	if err != nil {
		log.Fatalln("Unable to start the Workflow:", err)
	}

	log.Printf("WorkflowID: %s RunID: %s\n", we.GetID(), we.GetRunID())

	var result string

	err = we.Get(context.Background(), &result)

	if err != nil {
		log.Fatalln("Unable to get Workflow result:", err)
	}

	log.Println(result)
}

// @@@SNIPEND
