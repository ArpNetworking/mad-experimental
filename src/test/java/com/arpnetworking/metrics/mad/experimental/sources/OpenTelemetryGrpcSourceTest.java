package com.arpnetworking.metrics.mad.experimental.sources;

import akka.util.ByteString;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.exporter.internal.otlp.metrics.MetricsRequestMarshaler;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.MetricsService;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class OpenTelemetryGrpcSourceTest {
    @Test
    public void test() throws IOException, InterruptedException {
        final InMemoryMetricReader metricReader = InMemoryMetricReader.createDelta();
        final SdkMeterProvider mp = SdkMeterProvider.builder().registerMetricReader(metricReader).build();
        OpenTelemetrySdk.builder().setMeterProvider(mp).buildAndRegisterGlobal();

        final Meter meter = mp.meterBuilder("mad-experimental").setSchemaUrl("mad").build();
        final DoubleHistogram histo = meter.histogramBuilder("my_histogram").build();

        final Attributes attrs = Attributes.of(
                AttributeKey.stringKey("service"),
                "t_service",
                AttributeKey.stringKey("host"),
                "l_host",
                AttributeKey.stringKey("cluster"),
                "t_cluster");
        histo.record(1.0, attrs);
        histo.record(2.0, attrs);
        histo.record(3.0, attrs);
        histo.record(58.0, attrs);
        final MetricsRequestMarshaler marshaller = MetricsRequestMarshaler.create(metricReader.collectAllMetrics());
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        marshaller.writeBinaryTo(stream);

        ExportMetricsServiceRequest request = MetricsService.Serializers.ExportMetricsServiceRequestSerializer.deserialize(ByteString.fromArray(stream.toByteArray()));


    }

}
