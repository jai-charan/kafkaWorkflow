package app

const KafkaWorkflowTaskQueueName = "KAFKA_WORKFLOW_TASK_QUEUE"

type ProductData struct {
	Id             int
	Name           string
	Color          string
	AdditionalData struct {
		KafkaMessage string
	}
}

type ApiResponse struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Data struct {
		Color    string `json:"color"`
		Capacity string `json:"capacity"`
	} `json:"data"`
}
