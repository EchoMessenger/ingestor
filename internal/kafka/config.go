// Package kafka предоставляет общий конструктор sarama.Config
// с TLS и SASL настройками. Используется и consumer'ом и DLQ producer'ом
// чтобы не дублировать код и не получать EOF при подключении к защищённому брокеру.
package kafka

import (
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"strings"

	"github.com/EchoMessenger/ingestor/internal/config"
	"github.com/IBM/sarama"
	"github.com/xdg-go/scram"
)

var (
	sha256Hash scram.HashGeneratorFcn = sha256.New
	sha512Hash scram.HashGeneratorFcn = sha512.New
)

type xdgSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func (x *xdgSCRAMClient) Begin(u, p, a string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(u, p, a)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}
func (x *xdgSCRAMClient) Step(c string) (string, error) { return x.ClientConversation.Step(c) }
func (x *xdgSCRAMClient) Done() bool                    { return x.ClientConversation.Done() }

// NewSaramaConfig возвращает sarama.Config с применёнными TLS и SASL
// настройками из конфига сервиса. Вызывается и consumer'ом и DLQ producer'ом.
func NewSaramaConfig(cfg *config.Config) *sarama.Config {
	sc := sarama.NewConfig()

	if cfg.KafkaTLSEnable {
		sc.Net.TLS.Enable = true
		sc.Net.TLS.Config = &tls.Config{
			InsecureSkipVerify: false,
		}
	}

	if cfg.KafkaSASLEnable {
		sc.Net.SASL.Enable = true
		sc.Net.SASL.User = cfg.KafkaSASLUsername
		sc.Net.SASL.Password = cfg.KafkaSASLPassword

		switch strings.ToUpper(cfg.KafkaSASLMechanism) {
		case "SCRAM-SHA-256":
			sc.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
			sc.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &xdgSCRAMClient{HashGeneratorFcn: sha256Hash}
			}
		case "SCRAM-SHA-512":
			sc.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
			sc.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &xdgSCRAMClient{HashGeneratorFcn: sha512Hash}
			}
		default:
			sc.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		}
	}

	return sc
}