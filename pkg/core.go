package kafka

import (
	"context"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

func New(logger *zap.Logger, config *ConfigEntity) (*CoreEntity, error) {
	core := &CoreEntity{
		tcp:  kafka.TCP(config.Address...),
		addr: config.Address,

		WriterMap:   make(map[string]*kafka.Writer),
		ConsumerMap: make(map[string]*kafka.ConsumerGroup),

		logger: logger,
	}

	core.install(config)

	return core, nil
}

func (core *CoreEntity) CreateTopics(topics []string) {
	var topicConfig []kafka.TopicConfig
	for _, topic := range topics {
		topicConfig = append(topicConfig, kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: len(core.addr),
		})

		core.WriterMap[topic] = &kafka.Writer{
			Addr:      core.Cli.Addr,
			Topic:     topic,
			Balancer:  &kafka.LeastBytes{},
			Transport: core.Cli.Transport,
		}
	}
	if _, err := core.Cli.CreateTopics(context.Background(), &kafka.CreateTopicsRequest{Addr: core.Cli.Addr, Topics: topicConfig}); err != nil {
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
