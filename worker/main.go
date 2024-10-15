package main

import (
	"log"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"money-transfer-project-template-go/app"
)

// @@@SNIPSTART money-transfer-project-template-go-worker
func main() {

	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalln("Unable to create Temporal client.", err)
	}
	defer c.Close()

	w := worker.New(c, app.KafkaWorkflowTaskQueueName, worker.Options{})

	// This worker hosts both Workflow and Activity functions.
	w.RegisterWorkflow(app.KafkaWorkflow)

	w.RegisterActivity(app.ConsumeKafkaMessage)
	w.RegisterActivity(app.FetchData)
	w.RegisterActivity(app.PublishKafkaMessage)

	// Start listening to the Task Queue.
	err = w.Run(worker.InterruptCh())
	if err != nil {
		log.Fatalln("unable to start Worker", err)
	}
}

// @@@SNIPEND
