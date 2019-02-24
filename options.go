package file

import "go.uber.org/zap"

type Option func(*Memory) error

func WithLogger(logger *zap.Logger) Option {
	return func(memory *Memory) error {
		memory.logger = logger
		return nil
	}
}

// IDEA: encrypted brain?
// IDEA: only decrypt keys on demand?
