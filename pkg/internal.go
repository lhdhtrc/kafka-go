package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"os"
	"time"
)

func (core *CoreEntity) install(config *ConfigEntity) *CoreEntity {
	logPrefix := "install kafka"
	core.logger.Info(fmt.Sprintf("%s %s", logPrefix, "start ->"))

	var transport kafka.Transport
	if config.Account != "" && config.Password != "" {
		transport.SASL = &plain.Mechanism{Username: config.Account, Password: config.Password}
	}
	if config.Tls.CaCert != "" && config.Tls.ClientCert != "" && config.Tls.ClientCertKey != "" {
		certPool := x509.NewCertPool()
		CAFile, CAErr := os.ReadFile(config.Tls.CaCert)
		if CAErr != nil {
			core.logger.Error(CAErr.Error())
			return nil
		}
		certPool.AppendCertsFromPEM(CAFile)

		clientCert, clientCertErr := tls.LoadX509KeyPair(config.Tls.ClientCert, config.Tls.ClientCertKey)
		if clientCertErr != nil {
			core.logger.Error(clientCertErr.Error())
			return nil
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

	core.logger.Info(fmt.Sprintf("%s %s", logPrefix, "success ->"))

	return core
}
