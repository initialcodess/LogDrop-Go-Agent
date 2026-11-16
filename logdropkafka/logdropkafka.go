// Package logdropkafka traces Kafka messages sent and processed with github.com/segmentio/kafka-go for LogDrop, and
// passes the trace on in the message headers, so a message's processing continues the trace that sent it. Tracing
// starts with logdrop.Start; message keys and values are never recorded.
//
//	err := logdropkafka.WriteMessages(ctx, writer, kafka.Message{Value: body})   // instead of writer.WriteMessages
//	err := logdropkafka.Process(ctx, message, handle)                            // around the work for one message
package logdropkafka

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// ScopeName is the instrumentation scope of the spans.
const ScopeName = "github.com/initialcodess/LogDrop-Go-Agent/logdropkafka"

type headers struct{ message *kafka.Message }

func (h headers) Get(key string) string {
	for _, header := range h.message.Headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}

func (h headers) Set(key, value string) {
	for i, header := range h.message.Headers {
		if header.Key == key {
			h.message.Headers[i].Value = []byte(value)
			return
		}
	}
	h.message.Headers = append(h.message.Headers, kafka.Header{Key: key, Value: []byte(value)})
}

func (h headers) Keys() []string {
	keys := make([]string, 0, len(h.message.Headers))
	for _, header := range h.message.Headers {
		keys = append(keys, header.Key)
	}
	return keys
}

func tracer() trace.Tracer { return otel.Tracer(ScopeName) }

func destination(writer *kafka.Writer, message kafka.Message) string {
	if message.Topic != "" {
		return message.Topic
	}
	return writer.Topic
}

func serverAttributes(addr net.Addr) []attribute.KeyValue {
	if addr == nil {
		return nil
	}
	host, port, err := net.SplitHostPort(addr.String())
	if err != nil {
		return []attribute.KeyValue{attribute.String("server.address", addr.String())}
	}
	attributes := []attribute.KeyValue{attribute.String("server.address", host)}
	if p, err := strconv.Atoi(port); err == nil {
		attributes = append(attributes, attribute.Int("server.port", p))
	}
	return attributes
}

func recordError(span trace.Span, err error) {
	span.SetStatus(codes.Error, "")
	span.AddEvent("exception", trace.WithAttributes(attribute.String("exception.type", fmt.Sprintf("%T", err))))
}

// WriteMessages sends messages with writer like writer.WriteMessages: each message gets a producer span and carries
// the trace in its headers.
func WriteMessages(ctx context.Context, writer *kafka.Writer, messages ...kafka.Message) error {
	spans := make([]trace.Span, len(messages))
	for i := range messages {
		topic := destination(writer, messages[i])
		attributes := append([]attribute.KeyValue{
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.operation.name", "publish"),
			attribute.String("messaging.operation.type", "send"),
			attribute.String("messaging.destination.name", topic),
		}, serverAttributes(writer.Addr)...)
		spanCtx, span := tracer().Start(ctx, topic+" publish", trace.WithSpanKind(trace.SpanKindProducer),
			trace.WithAttributes(attributes...))
		otel.GetTextMapPropagator().Inject(spanCtx, headers{&messages[i]})
		spans[i] = span
	}
	err := writer.WriteMessages(ctx, messages...)
	for _, span := range spans {
		if err != nil {
			recordError(span, err)
		}
		span.End()
	}
	return err
}

// StartProcess starts the consumer span of message, continuing the trace in its headers. End the span when the
// message is processed.
func StartProcess(ctx context.Context, message kafka.Message) (context.Context, trace.Span) {
	parent := otel.GetTextMapPropagator().Extract(ctx, headers{&message})
	return tracer().Start(parent, message.Topic+" process", trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.operation.name", "process"),
			attribute.String("messaging.operation.type", "process"),
			attribute.String("messaging.destination.name", message.Topic),
		))
}

// Process runs work for message in its consumer span. An error that work returns is recorded with its type only and
// passed on.
func Process(ctx context.Context, message kafka.Message, work func(ctx context.Context) error) error {
	ctx, span := StartProcess(ctx, message)
	defer span.End()
	err := work(ctx)
	if err != nil {
		recordError(span, err)
	}
	return err
}
