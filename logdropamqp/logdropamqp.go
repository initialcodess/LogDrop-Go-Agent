// Package logdropamqp traces RabbitMQ messages published and processed with github.com/rabbitmq/amqp091-go for
// LogDrop, and passes the trace on in the message headers, so a message's processing continues the trace that
// published it. Tracing starts with logdrop.Start; routing keys and message bodies are never recorded.
//
//	err := logdropamqp.Publish(ctx, channel, "orders", "created", false, false, msg)   // instead of PublishWithContext
//	err := logdropamqp.Process(ctx, delivery, handle)                                 // around the work for one delivery
package logdropamqp

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// ScopeName is the instrumentation scope of the spans.
const ScopeName = "github.com/initialcodess/LogDrop-Go-Agent/logdropamqp"

type headers struct{ table *amqp.Table }

func (h headers) Get(key string) string {
	if value, ok := (*h.table)[key].(string); ok {
		return value
	}
	return ""
}

func (h headers) Set(key, value string) {
	if *h.table == nil {
		*h.table = amqp.Table{}
	}
	(*h.table)[key] = value
}

func (h headers) Keys() []string {
	keys := make([]string, 0, len(*h.table))
	for key := range *h.table {
		keys = append(keys, key)
	}
	return keys
}

func tracer() trace.Tracer { return otel.Tracer(ScopeName) }

// destinationName is the exchange, or "<default>" for the default exchange, like the Java agent.
func destinationName(exchange string) string {
	if exchange == "" {
		return "<default>"
	}
	return exchange
}

func recordError(span trace.Span, err error) {
	span.SetStatus(codes.Error, "")
	span.AddEvent("exception", trace.WithAttributes(attribute.String("exception.type", fmt.Sprintf("%T", err))))
}

func attributes(operation, exchange string) trace.SpanStartOption {
	return trace.WithAttributes(
		attribute.String("messaging.system", "rabbitmq"),
		attribute.String("messaging.operation.name", operation),
		attribute.String("messaging.operation.type", operation),
		attribute.String("messaging.destination.name", destinationName(exchange)),
	)
}

// Publish publishes msg like channel.PublishWithContext, in a producer span, with the trace in its headers.
func Publish(ctx context.Context, channel *amqp.Channel, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	ctx, span := tracer().Start(ctx, destinationName(exchange)+" publish", trace.WithSpanKind(trace.SpanKindProducer),
		attributes("publish", exchange))
	defer span.End()
	table := msg.Headers
	otel.GetTextMapPropagator().Inject(ctx, headers{&table})
	msg.Headers = table
	err := channel.PublishWithContext(ctx, exchange, key, mandatory, immediate, msg)
	if err != nil {
		recordError(span, err)
	}
	return err
}

// StartProcess starts the consumer span of delivery, continuing the trace in its headers. End the span when the
// delivery is processed.
func StartProcess(ctx context.Context, delivery amqp.Delivery) (context.Context, trace.Span) {
	table := delivery.Headers
	parent := otel.GetTextMapPropagator().Extract(ctx, headers{&table})
	return tracer().Start(parent, destinationName(delivery.Exchange)+" process", trace.WithSpanKind(trace.SpanKindConsumer),
		attributes("process", delivery.Exchange))
}

// Process runs work for delivery in its consumer span. An error that work returns is recorded with its type only and
// passed on.
func Process(ctx context.Context, delivery amqp.Delivery, work func(ctx context.Context) error) error {
	ctx, span := StartProcess(ctx, delivery)
	defer span.End()
	err := work(ctx)
	if err != nil {
		recordError(span, err)
	}
	return err
}
