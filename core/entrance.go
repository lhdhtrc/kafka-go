package core

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"github.com/lhdhtrc/kafka-go/model"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"net"
	"os"
	"time"
)

type KafkaCoreEntity struct {
	tcp  net.Addr
	addr []string

	Cli         *kafka.Client
	Conn        *kafka.Dialer
	WriterMap   map[string]*kafka.Writer
	ConsumerMap map[string]*kafka.ConsumerGroup
}

func New(config model.ConfigEntity) (*KafkaCoreEntity, error) {
	core := &KafkaCoreEntity{
		tcp:         kafka.TCP(config.Address...),
		addr:        config.Address,
		WriterMap:   make(map[string]*kafka.Writer),
		ConsumerMap: make(map[string]*kafka.ConsumerGroup),
	}

	var transport kafka.Transport
	if config.Account != "" && config.Password != "" {
		transport.SASL = &plain.Mechanism{Username: config.Account, Password: config.Password}
	}
	if config.Tls.CaCert != "" && config.Tls.ClientCert != "" && config.Tls.ClientCertKey != "" {
		certPool := x509.NewCertPool()
		CAFile, CAErr := os.ReadFile(config.Tls.CaCert)
		if CAErr != nil {
			return nil, CAErr
		}
		certPool.AppendCertsFromPEM(CAFile)

		clientCert, clientCertErr := tls.LoadX509KeyPair(config.Tls.ClientCert, config.Tls.ClientCertKey)
		if clientCertErr != nil {
			return nil, clientCertErr
		}

		transport.TLS = &tls.Config{
			Certificates: []tls.Certificate{clientCert},
			RootCAs:      certPool,
		}
	}

	timeout := 10 * time.Second
	core.Cli = &kafka.Client{
		Timeout:   timeout,
		Addr:      core.tcp,
		Transport: &transport,
	}
	core.Conn = &kafka.Dialer{
		Timeout:       timeout,
		TLS:           transport.TLS,
		SASLMechanism: transport.SASL,
	}

	return core, nil
}

func (s *KafkaCoreEntity) CreateTopics(topics []string) error {
	var topicConfig []kafka.TopicConfig
	for _, topic := range topics {
		topicConfig = append(topicConfig, kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: len(s.addr),
		})

		s.WriterMap[topic] = &kafka.Writer{
			Addr:      s.Cli.Addr,
			Topic:     topic,
			Balancer:  &kafka.LeastBytes{},
			Transport: s.Cli.Transport,
		}
	}
	if _, err := s.Cli.CreateTopics(context.Background(), &kafka.CreateTopicsRequest{Addr: s.Cli.Addr, Topics: topicConfig}); err != nil {
		return err
	}
	return nil
}

func (s *KafkaCoreEntity) Production(topic string, message []kafka.Message) error {
	v, ok := s.WriterMap[topic]
	if !ok {
		return errors.New("topic writer not found")
	}

	err := v.WriteMessages(context.Background(), message...)
	return err
}

func (s *KafkaCoreEntity) Consumption(topic string, handle func(read *kafka.Reader, message kafka.Message)) {
	ctx := context.Background()
	r := kafka.NewReader(kafka.ReaderConfig{
		GroupID:   "LhdhtMicroservice",
		Dialer:    s.Conn,
		Brokers:   s.addr,
		Topic:     topic,
		Partition: 0,
	})
	defer func(r *kafka.Reader) {
		err := r.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(r)

	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			break
		}
		handle(r, m)
	}
}
