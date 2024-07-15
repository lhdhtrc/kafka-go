## Kafka Go
Provides easy to use API to operate Kafka.

### How to use it?
`go get github.com/lhdhtrc/etcd-go`

```go
package main

import (
	kafka "github.com/lhdhtrc/kafka-go/pkg"
	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewProduction()
	instance := kafka.New(logger, kafka.ConfigEntity{})

	// How to create a theme?
	instance.InitTopic([]string{"emails"})
	
	// How to send a message?
	instance.Production("emails", []kafka.Message{
		{Key: []byte("admin@163.com"), Value: []byte("fadfdc")},
		{Key: []byte("admin@lhdht.cn"), Value: []byte("112.11")},
	})
	
	// How to consume messages?
	instance.Consumption(kafka.ConsumptionEntity{
		Topic: "emails",
		Handle: func(read *kafka.Reader, message kafka.Message) {
			if string(message.Key) == "admin@lhdht.cn" {
				if err := read.CommitMessages(context.Background(), message); err != nil {
					fmt.Println(err)
				}
			}
		},
		GroupId: "LhdhtKafka",
    })
}
```

### Finally
- If you feel good, click on star.
- If you have a good suggestion, please ask the issue.