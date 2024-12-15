package tracer

import (
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

type Tracer struct {
	cfg Config
	exp *jaeger.Exporter
	tp  *tracesdk.TracerProvider
}

func New(cfg Config) (*Tracer, error) {
	options := []jaeger.CollectorEndpointOption{
		jaeger.WithEndpoint(cfg.URL),
	}

	exp, err := jaeger.New(
		jaeger.WithCollectorEndpoint(
			options...,
		),
	)
	if err != nil {
		return nil, err
	}

	tp := tracesdk.NewTracerProvider(
		tracesdk.WithBatcher(exp),
		tracesdk.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(cfg.ServiceName),
		)),
	)

	return &Tracer{
		cfg: cfg,
		tp:  tp,
		exp: exp,
	}, nil
}

func (t *Tracer) GetTracerProvider() *tracesdk.TracerProvider {
	return t.tp
}

func (t *Tracer) GetExporter() *jaeger.Exporter {
	return t.exp
}
