package app

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// @@@SNIPSTART money-transfer-project-template-go-workflow
func KafkaWorkflow(ctx workflow.Context) (string, error) {

	// RetryPolicy specifies how to automatically handle retries if an Activity fails.
	retrypolicy := &temporal.RetryPolicy{
		InitialInterval:        time.Second,
		BackoffCoefficient:     2.0,
		MaximumInterval:        100 * time.Second,
		MaximumAttempts:        500, // 0 is unlimited retries
		NonRetryableErrorTypes: []string{"InvalidAccountError", "InsufficientFundsError"},
	}

	options := workflow.ActivityOptions{
		// Timeout options specify when to automatically timeout Activity functions.
		StartToCloseTimeout: time.Minute,
		// Optionally provide a customized RetryPolicy.
		// Temporal retries failed Activities by default.
		RetryPolicy: retrypolicy,
	}

	// Apply the options.
	ctx = workflow.WithActivityOptions(ctx, options)

	//Listen to Kafka Topic
	var kafkaMessage string
	var topic = "gadget"
	kafkaMessageErr := workflow.ExecuteActivity(ctx, ConsumeKafkaMessage, topic).Get(ctx, &kafkaMessage)
	if kafkaMessageErr != nil {
		return "", kafkaMessageErr
	}

	// Fetch Device data
	var dataOutput ProductData

	fetchDataErr := workflow.ExecuteActivity(ctx, FetchData, kafkaMessage).Get(ctx, &dataOutput)
	if fetchDataErr != nil {
		return "", fetchDataErr
	}

	//Publish the  Message to Kafka Topic
	var publishedMessage string
	PublishMessageErr := workflow.ExecuteActivity(ctx, PublishKafkaMessage, topic, dataOutput).Get(ctx, &publishedMessage)
	if PublishMessageErr != nil {
		return "", PublishMessageErr
	}

	result := fmt.Sprintf("Workflow Successfully completed and the message %v is published to Kafka topic", publishedMessage)
	return result, nil

}

// @@@SNIPEND
