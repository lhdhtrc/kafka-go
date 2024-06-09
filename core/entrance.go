package core

import (
	"crypto/tls"
	"crypto/x509"
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
	Cli  *kafka.Client

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

	core.Cli = &kafka.Client{
		Addr:      core.tcp,
		Timeout:   10 * time.Second,
		Transport: &transport,
	}

	return core, nil
}
