package kafka

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

func New(logger *zap.Logger, config *ConfigEntity) (*CoreEntity, error) {
	ctx, cancel := context.WithCancel(context.Background())

	core := &CoreEntity{
		ctx:    ctx,
		cancel: cancel,

		tcp:      kafka.TCP(config.Address...),
		addr:     config.Address,
		maxRetry: config.MaxRetry,

		writerMap: make(map[string]*kafka.Writer),

		logger: logger,
	}

	core.install(config)

	return core, nil
}

func (core *CoreEntity) InitTopics(topics []string) {
	var topicConfig []kafka.TopicConfig
	for _, topic := range topics {
		topicConfig = append(topicConfig, kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: len(core.addr),
		})

		core.writerMap[topic] = &kafka.Writer{
			Addr:      core.cli.Addr,
			Topic:     topic,
			Balancer:  &kafka.LeastBytes{},
			Transport: core.cli.Transport,
		}
	}
	if _, err := core.cli.CreateTopics(context.Background(), &kafka.CreateTopicsRequest{Addr: core.cli.Addr, Topics: topicConfig}); err != nil {
		core.logger.Error(err.Error())
	}
}

func (core *CoreEntity) Production(topic string, message []kafka.Message) {
	v, ok := core.writerMap[topic]
	if !ok {
		core.logger.Error("topic writer not found")
		return
	}

	err := v.WriteMessages(context.Background(), message...)
	if err != nil {
		core.logger.Error(err.Error())
	}
}

func (core *CoreEntity) Consumption(config *ConsumptionEntity) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Dialer:  core.conn,
		Brokers: core.addr,
		GroupID: config.GroupId,
		Topic:   config.Topic,
	})
	defer func(r *kafka.Reader) { _ = r.Close() }(r)

	for {
		m, err := r.FetchMessage(core.ctx)
		if err != nil {
			core.retryConsumption(config)
			core.logger.Error(err.Error())
			break
		}
		config.Handle(r, m)
		core.countRetry = 0
	}
}

func (core *CoreEntity) Uninstall() {
	core.cancel()

	fmt.Println("uninstall kafka success")
}
