//go:build integration
// +build integration

package mqtt

import (
	"context"
	"net/url"
	"testing"

	"github.com/eclipse/paho.golang/paho"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testTopic = "testTopic"

func TestAsyncProducer_SendMessage_Close(t *testing.T) {
	mtr := mocktracer.New()
	defer mtr.Reset()
	opentracing.SetGlobalTracer(mtr)

	u, err := url.Parse("tcp://broker.emqx.io:1883")
	require.NoError(t, err)

	cfg, err := DefaultConfig([]*url.URL{u}, "test-client-id")
	require.NoError(t, err)

	pub, err := New(cfg)
	require.NoError(t, err)

	msg := &paho.Publish{
		QoS:     1,
		Topic:   testTopic,
		Payload: []byte("123"),
	}

	rsp, err := pub.Publish(context.Background(), msg)
	require.NoError(t, err)
	assert.NotNil(t, rsp)

	require.NoError(t, pub.Disconnect(context.Background()))
	assert.Len(t, mtr.FinishedSpans(), 1)

	expected := map[string]interface{}{
		"component": "kafka-async-producer",
		"error":     false,
		"span.kind": ext.SpanKindEnum("producer"),
		"topic":     testTopic,
		"type":      "async",
		"version":   "dev",
	}
	assert.Equal(t, expected, mtr.FinishedSpans()[0].Tags())
	// Metrics
	// assert.Equal(t, 1, testutil.CollectAndCount(messageStatus, "component_kafka_producer_message_status"))
}
