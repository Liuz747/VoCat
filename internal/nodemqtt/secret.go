package nodemqtt

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const commandKeySuffix = ".node-mqtt.key"

var commandAAD = []byte("vocat-node-mqtt-command-v1")

type commandCipher struct{ aead cipher.AEAD }

type encryptedCommand struct {
	Ciphertext string `json:"_encrypted"`
}

func newCommandCipher(databasePath string) (*commandCipher, error) {
	key, err := loadOrCreateCommandKey(databasePath)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &commandCipher{aead: aead}, nil
}

func loadOrCreateCommandKey(databasePath string) ([]byte, error) {
	if strings.TrimSpace(databasePath) == "" {
		key := make([]byte, 32)
		_, err := rand.Read(key)
		return key, err
	}
	keyPath := filepath.Clean(databasePath) + commandKeySuffix
	encoded, err := os.ReadFile(keyPath)
	if err == nil {
		key, decodeErr := base64.StdEncoding.DecodeString(strings.TrimSpace(string(encoded)))
		if decodeErr != nil || len(key) != 32 {
			return nil, errors.New("node MQTT command encryption key is invalid")
		}
		if chmodErr := os.Chmod(keyPath, 0o600); chmodErr != nil {
			return nil, fmt.Errorf("secure node MQTT command key: %w", chmodErr)
		}
		return key, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read node MQTT command key: %w", err)
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create node MQTT command key: %w", err)
	}
	_, writeErr := file.WriteString(base64.StdEncoding.EncodeToString(key))
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if writeErr != nil {
		_ = os.Remove(keyPath)
		return nil, fmt.Errorf("write node MQTT command key: %w", writeErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close node MQTT command key: %w", closeErr)
	}
	return key, nil
}

func (value *commandCipher) encrypt(plaintext []byte) (json.RawMessage, error) {
	nonce := make([]byte, value.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	sealed := value.aead.Seal(nonce, nonce, plaintext, commandAAD)
	payload, err := json.Marshal(encryptedCommand{Ciphertext: base64.StdEncoding.EncodeToString(sealed)})
	return payload, err
}

func (value *commandCipher) decrypt(payload []byte) ([]byte, error) {
	var envelope encryptedCommand
	if err := decodeStrict(payload, &envelope); err != nil || strings.TrimSpace(envelope.Ciphertext) == "" {
		return nil, errors.New("invalid encrypted node MQTT command")
	}
	sealed, err := base64.StdEncoding.DecodeString(envelope.Ciphertext)
	if err != nil || len(sealed) < value.aead.NonceSize() {
		return nil, errors.New("invalid encrypted node MQTT command")
	}
	nonce := sealed[:value.aead.NonceSize()]
	plaintext, err := value.aead.Open(nil, nonce, sealed[value.aead.NonceSize():], commandAAD)
	if err != nil {
		return nil, errors.New("decrypt node MQTT command")
	}
	return plaintext, nil
}
