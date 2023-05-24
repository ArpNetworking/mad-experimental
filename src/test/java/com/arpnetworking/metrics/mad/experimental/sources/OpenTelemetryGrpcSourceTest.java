package com.arpnetworking.metrics.mad.experimental.sources;

import akka.util.ByteString;
import com.arpnetworking.metrics.mad.model.Metric;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.metrics.mad.model.statistics.MaxStatistic;
import com.arpnetworking.metrics.mad.model.statistics.Statistic;
import com.arpnetworking.metrics.mad.model.statistics.StatisticFactory;
import com.arpnetworking.tsdcore.model.CalculatedValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

public class OpenTelemetryGrpcSourceTest {
    @Test
    public void testHistograms() throws IOException, InterruptedException {
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

        final OpenTelemetryGrpcRecordParser parser = new OpenTelemetryGrpcRecordParser();
        ExportMetricsServiceRequest request = MetricsService.Serializers.ExportMetricsServiceRequestSerializer.deserialize(ByteString.fromArray(stream.toByteArray()));
        final List<Record> records = parser.parseRecords(request);
        // Assert on records
        Assert.assertEquals(1, records.size());
        final Record record = records.get(0);
        final ImmutableMap<String, String> dimensions = record.getDimensions();
        Assert.assertEquals("t_service", dimensions.get("service"));
        Assert.assertEquals("l_host", dimensions.get("host"));
        Assert.assertEquals("t_cluster", dimensions.get("cluster"));
        final ImmutableMap<String, ? extends Metric> metrics = record.getMetrics();
        Assert.assertEquals(1, metrics.size());
        final Metric metric = metrics.get("my_histogram");
        Assert.assertNotNull(metric);
        final ImmutableMap<Statistic, ImmutableList<CalculatedValue<?>>> statistics = metric.getStatistics();
        Assert.assertEquals(75d, statistics.get(new StatisticFactory().getStatistic("max")).get(0).getValue().getValue(), 0.01);
        Assert.assertEquals(4d, statistics.get(new StatisticFactory().getStatistic("count")).get(0).getValue().getValue(), 0.01);
        Assert.assertEquals(64d, statistics.get(new StatisticFactory().getStatistic("sum")).get(0).getValue().getValue(), 0.01);
    }

}
