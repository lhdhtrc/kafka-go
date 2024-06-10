## Kafka Go
Provides easy to use API to operate Kafka.

### Init Kafka
```go
c, e := core.New(model.ConfigEntity{
    Tls: model.TLSEntity{
        CaCert:        "./certs/ca.crt",
        ClientCert:    "./certs/client.crt",
        ClientCertKey: "./certs/client.key",
    },
    Account:  "lhdht",
    Password: "123456",
    Address:  []string{"127.0.0.1:10105"},
})
if e != nil {
    fmt.Println(e)
    return
}
```

### Create Topic
```go
c.CreateTopics([]string{"emails"})
```

### Production Message
```go
err := c.Production("emails", []kafka.Message{
    {Key: []byte("admin@163.com"), Value: []byte("fadfdc")},
    {Key: []byte("admin@lhdht.cn"), Value: []byte("112.11")},
})
if err != nil {
    fmt.Println(err)
    return
}
```

### Consumption Message
```go
c.Consumption("emails", func(read *kafka.Reader, message kafka.Message) {
    fmt.Println(string(message.Key))

    if string(message.Key) == "admin@lhdht.cn" {
        if err := read.CommitMessages(context.Background(), message); err != nil {
            fmt.Println(err)
        }
    }
})
```