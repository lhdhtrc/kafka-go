package model

type ConfigEntity struct {
	Tls      TLSEntity `json:"tls" bson:"tls" yaml:"tls" mapstructure:"tls"`
	Account  string    `json:"account" bson:"account" yaml:"account" mapstructure:"account"`
	Password string    `json:"password" bson:"password" yaml:"password" mapstructure:"password"`
	Address  []string  `json:"address" yaml:"address" mapstructure:"address"`
}
