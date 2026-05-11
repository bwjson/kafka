package producer

import (
	"context"
)

type Producer interface {
	Send(ctx context.Context, topic, key string, value any) error
	Close()
}
