package file

import (
	"encoding/json"
	"os"
	"sync"

	"github.com/go-joe/joe"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type Memory struct {
	path   string
	logger *zap.Logger

	mu   sync.RWMutex
	data map[string]string
}

func NewMemory(path string, opts ...Option) (*Memory, error) {
	memory := &Memory{
		path: path,
		data: map[string]string{},
	}

	for _, opt := range opts {
		err := opt(memory)
		if err != nil {
			return nil, err
		}
	}

	if memory.logger == nil {
		memory.logger = zap.NewNop()
	}

	memory.logger.Debug("Opening memory file", zap.String("path", path))
	f, err := os.Open(path)
	switch {
	case os.IsNotExist(err):
		memory.logger.Debug("File does not exist. Continuing with empty memory", zap.String("path", path))
	case err != nil:
		return nil, errors.Wrap(err, "failed to open file")
	default:
		memory.logger.Debug("Decoding JSON from memory file", zap.String("path", path))
		err := json.NewDecoder(f).Decode(&memory.data)
		_ = f.Close()
		if err != nil {
			return nil, errors.Wrap(err, "failed decode data as JSON")
		}
	}

	memory.logger.Info("Memory initialized successfully",
		zap.String("path", path),
		zap.Int("num_memories", len(memory.data)),
	)

	return memory, nil
}

func MemoryOption(path string) joe.Option {
	return func(conf *joe.Config) error {
		var opts []Option
		if conf.Logger != nil {
			opts = append(opts, WithLogger(conf.Logger.Named("memory")))
		}

		memory, err := NewMemory(path, opts...)
		if err != nil {
			return err
		}

		conf.Memory = memory
		return nil
	}
}

func (m *Memory) Set(key, value string) error {
	if m.data == nil {
		return errors.New("brain was already shut down")
	}

	m.data[key] = value
	err := m.persist()

	return err
}

func (m *Memory) Get(key string) (string, bool, error) {
	if m.data == nil {
		return "", false, errors.New("brain was already shut down")
	}

	value, ok := m.data[key]
	return value, ok, nil
}

func (m *Memory) Delete(key string) (bool, error) {
	if m.data == nil {
		return false, errors.New("brain was already shut down")
	}

	_, ok := m.data[key]
	delete(m.data, key)
	err := m.persist()

	return ok, err
}

func (m *Memory) Memories() (map[string]string, error) {
	if m.data == nil {
		return nil, errors.New("brain was already shut down")
	}

	data := make(map[string]string, len(m.data))
	for k, v := range m.data {
		data[k] = v
	}

	return data, nil
}

func (m *Memory) Close() error {
	if m.data == nil {
		return errors.New("brain was already closed")
	}

	m.data = nil

	return nil
}

func (m *Memory) persist() error {
	f, err := os.OpenFile(m.path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0660)
	if err != nil {
		return errors.Wrap(err, "failed to open file to persist data")
	}

	err = json.NewEncoder(f).Encode(m.data)
	if err != nil {
		_ = f.Close()
		return errors.Wrap(err, "failed to encode data as JSON")
	}

	err = f.Close()
	if err != nil {
		return errors.Wrap(err, "failed to close file; data might not have been fully persisted to disk")
	}

	return nil
}
