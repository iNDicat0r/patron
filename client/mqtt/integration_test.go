//go:build integration
// +build integration

package mqtt

import (
	"context"
	"encoding/json"
	"net/url"
	"testing"

	"github.com/eclipse/paho.golang/paho"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testTopic = "testTopic"
	hiveMQURL = "tcp://localhost:1883"
)

func TestPublish(t *testing.T) {
	mtr := mocktracer.New()
	defer mtr.Reset()
	opentracing.SetGlobalTracer(mtr)

	u, err := url.Parse(hiveMQURL)
	require.NoError(t, err)

	ctx, cnl := context.WithCancel(context.Background())
	defer cnl()

	cfg, err := DefaultConfig([]*url.URL{u}, "test-client-id")
	require.NoError(t, err)

	pub, err := New(ctx, cfg)
	require.NoError(t, err)

	payload, err := json.Marshal(struct{ Count uint64 }{Count: 1})
	require.NoError(t, err)

	msg := &paho.Publish{
		QoS:     1,
		Topic:   testTopic,
		Payload: payload,
	}

	rsp, err := pub.Publish(ctx, msg)
	require.NoError(t, err)
	assert.NotNil(t, rsp)

	require.NoError(t, pub.Disconnect(ctx))

	// Traces
	assert.Len(t, mtr.FinishedSpans(), 1)

	expected := map[string]interface{}{
		"component": "mqtt-publisher",
		"error":     false,
		"span.kind": ext.SpanKindEnum("producer"),
		"topic":     testTopic,
		"version":   "dev",
	}
	assert.Equal(t, expected, mtr.FinishedSpans()[0].Tags())
	// Metrics
	assert.Equal(t, 1, testutil.CollectAndCount(publishDurationMetrics, "client_mqtt_publish_duration_seconds"))
}
