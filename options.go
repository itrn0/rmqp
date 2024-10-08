package rmqp

import (
	"log/slog"
	"time"
)

type Options struct {
	Heartbeat      time.Duration
	ReconnectDelay time.Duration
	Logger         *slog.Logger
	ContentType    string
	ConnectionName string
	Capacity       int
	Debug          bool
}

func handleOptions(options *Options) *Options {
	if options == nil {
		options = &Options{}
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	if options.ReconnectDelay == 0 {
		options.ReconnectDelay = 3 * time.Second
	}
	if options.Heartbeat == 0 {
		options.Heartbeat = 10 * time.Second
	}
	if options.Heartbeat < 5*time.Second {
		options.Logger.Warn("Heartbeat is too low, setting to 5 seconds")
		options.Heartbeat = 5 * time.Second
	}
	if options.Capacity <= 0 {
		options.Capacity = 100
	}
	if options.ContentType == "" {
		options.ContentType = "text/plain"
	}
	return options
}
