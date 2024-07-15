package kafka

import (
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"net"
)

type TLSEntity struct {
	CaCert        string `json:"ca_cert" bson:"ca_cert" yaml:"ca_cert" mapstructure:"ca_cert"`
	ClientCert    string `json:"client_cert" bson:"client_cert" yaml:"client_cert" mapstructure:"client_cert"`
	ClientCertKey string `json:"client_cert_key" bson:"client_cert_key" yaml:"client_cert_key" mapstructure:"client_cert_key"`
}

type ConfigEntity struct {
	Tls      TLSEntity `json:"tls" bson:"tls" yaml:"tls" mapstructure:"tls"`
	Account  string    `json:"account" bson:"account" yaml:"account" mapstructure:"account"`
	Password string    `json:"password" bson:"password" yaml:"password" mapstructure:"password"`
	Address  []string  `json:"address" yaml:"address" mapstructure:"address"`
}

type CoreEntity struct {
	tcp    net.Addr
	addr   []string
	logger *zap.Logger

	Cli         *kafka.Client
	Conn        *kafka.Dialer
	WriterMap   map[string]*kafka.Writer
	ConsumerMap map[string]*kafka.ConsumerGroup
}
