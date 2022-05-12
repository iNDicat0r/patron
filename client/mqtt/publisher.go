package mqtt

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
)

func DefaultConfig(brokerURLs []*url.URL, clientID string) (autopaho.ClientConfig, error) {
	if len(brokerURLs) == 0 {
		return autopaho.ClientConfig{}, errors.New("no broker URLs provided")
	}
	if clientID == "" {
		return autopaho.ClientConfig{}, errors.New("no client id provided")
	}

	// TODO: setup callback and log info

	return autopaho.ClientConfig{
		BrokerUrls:        brokerURLs,
		TlsCfg:            nil,
		KeepAlive:         30,
		ConnectRetryDelay: 5 * time.Second,
		ConnectTimeout:    1 * time.Second,
		WebSocketCfg:      nil,
		OnConnectionUp:    nil,
		OnConnectError:    nil,
		Debug:             nil,
		PahoDebug:         nil,
		ClientConfig: paho.ClientConfig{
			ClientID:                   clientID,
			Conn:                       nil,
			MIDs:                       nil,
			AuthHandler:                nil,
			PingHandler:                nil,
			Router:                     nil,
			Persistence:                nil,
			PacketTimeout:              0,
			OnServerDisconnect:         nil,
			OnClientError:              nil,
			PublishHook:                nil,
			EnableManualAcknowledgment: false,
			SendAcksInterval:           0,
		},
	}, nil
}

type Publisher struct {
	cm *autopaho.ConnectionManager
}

func New(cfg autopaho.ClientConfig) (*Publisher, error) {
	ctx, cnl := context.WithCancel(context.Background())
	defer cnl()

	cm, err := autopaho.NewConnection(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection manager: %w", err)
	}

	err = cm.AwaitConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("connection is not up: %w", err)
	}

	return &Publisher{cm: cm}, nil
}

func (p *Publisher) Publish(ctx context.Context, pub *paho.Publish) (*paho.PublishResponse, error) {
	err := p.cm.AwaitConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("connection is not up: %w", err)
	}

	// TODO: tracing, metrics
	rsp, err := p.cm.Publish(ctx, pub)
	if err != nil {
		return nil, fmt.Errorf("failed to publish message: %w", err)
	}

	return rsp, nil
}

func (p *Publisher) Disconnect(ctx context.Context) error {
	return p.cm.Disconnect(ctx)
}
