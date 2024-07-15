package kafka

import (
	"context"
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
	v, ok := core.WriterMap[topic]
	if !ok {
		core.logger.Error("topic writer not found")
		return
	}

	err := v.WriteMessages(context.Background(), message...)
	if err != nil {
		core.logger.Error(err.Error())
		return
	}
}

func (core *CoreEntity) Consumption(topic string, handle func(read *kafka.Reader, message kafka.Message)) {
	r := kafka.NewReader(kafka.ReaderConfig{
		GroupID:   "LhdhtKafka",
		Dialer:    core.Conn,
		Brokers:   core.addr,
		Topic:     topic,
		Partition: 0,
	})
	defer func(r *kafka.Reader) { _ = r.Close() }(r)

	for {
		m, err := r.FetchMessage(context.Background())
		if err != nil {
			break
		}
		handle(r, m)
	}
}
