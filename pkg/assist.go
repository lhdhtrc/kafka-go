package kafka

import "github.com/segmentio/kafka-go"

func (core *CoreEntity) Cli() *kafka.Client {
	return core.cli
}

func (core *CoreEntity) Conn() *kafka.Dialer {
	return core.conn
}

func (core *CoreEntity) Topic(key string) *kafka.Writer {
	topic, ok := core.writerMap[key]
	if !ok {
		return nil
	}
	return topic
}
