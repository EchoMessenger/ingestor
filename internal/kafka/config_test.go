package kafka

import (
	"testing"

	"github.com/EchoMessenger/ingestor/internal/config"
	"github.com/IBM/sarama"
)

func TestNewSaramaConfigAppliesTLSAndSASL(t *testing.T) {
	tests := []struct {
		name      string
		mechanism string
		want      sarama.SASLMechanism
		wantSCRAM bool
	}{
		{"plain default", "PLAIN", sarama.SASLTypePlaintext, false},
		{"scram sha256", "SCRAM-SHA-256", sarama.SASLTypeSCRAMSHA256, true},
		{"scram sha512", "SCRAM-SHA-512", sarama.SASLTypeSCRAMSHA512, true},
		{"unknown fallback", "unknown", sarama.SASLTypePlaintext, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := NewSaramaConfig(&config.Config{
				KafkaTLSEnable:     true,
				KafkaSASLEnable:    true,
				KafkaSASLMechanism: tt.mechanism,
				KafkaSASLUsername:  "user",
				KafkaSASLPassword:  "pass",
			})
			if !sc.Net.TLS.Enable || sc.Net.TLS.Config == nil {
				t.Fatalf("TLS was not enabled")
			}
			if !sc.Net.SASL.Enable || sc.Net.SASL.User != "user" || sc.Net.SASL.Password != "pass" {
				t.Fatalf("SASL credentials were not applied: %#v", sc.Net.SASL)
			}
			if sc.Net.SASL.Mechanism != tt.want {
				t.Fatalf("mechanism = %s, want %s", sc.Net.SASL.Mechanism, tt.want)
			}
			if (sc.Net.SASL.SCRAMClientGeneratorFunc != nil) != tt.wantSCRAM {
				t.Fatalf("SCRAM generator presence = %v, want %v", sc.Net.SASL.SCRAMClientGeneratorFunc != nil, tt.wantSCRAM)
			}
		})
	}
}

func TestNewSaramaConfigLeavesSecurityDisabledByDefault(t *testing.T) {
	sc := NewSaramaConfig(&config.Config{})
	if sc.Net.TLS.Enable {
		t.Fatalf("TLS enabled by default")
	}
	if sc.Net.SASL.Enable {
		t.Fatalf("SASL enabled by default")
	}
}

func TestXDGSCRAMClientBeginAndDone(t *testing.T) {
	client := &xdgSCRAMClient{HashGeneratorFcn: sha256Hash}
	if err := client.Begin("user", "pass", ""); err != nil {
		t.Fatalf("Begin returned error: %v", err)
	}
	if client.Client == nil || client.ClientConversation == nil {
		t.Fatalf("client was not initialized")
	}
	if client.Done() {
		t.Fatalf("Done = true before conversation completed")
	}
}
