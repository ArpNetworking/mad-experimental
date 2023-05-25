package com.arpnetworking.metrics.mad.experimental.sources;

import akka.util.ByteString;
import com.arpnetworking.metrics.mad.model.Metric;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.metrics.mad.model.statistics.Statistic;
import com.arpnetworking.metrics.mad.model.statistics.StatisticFactory;
import com.arpnetworking.tsdcore.model.CalculatedValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.exporter.internal.otlp.metrics.MetricsRequestMarshaler;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.MetricsService;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.View;
import io.opentelemetry.sdk.metrics.internal.aggregator.HistogramIndexer;
import io.opentelemetry.sdk.metrics.internal.view.Base2ExponentialHistogramAggregation;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

public class OpenTelemetryGrpcSourceTest {
    private InMemoryMetricReader _metricReader;
    private SdkMeterProvider _metricProvider;
    private OpenTelemetrySdk _openTelemetrySdk;
    private static final double LOG_BASE2_E = 1D / Math.log(2);


    @Before
    public void setup() {
        GlobalOpenTelemetry.resetForTest();
        _metricReader = InMemoryMetricReader.createDelta();
        _metricProvider = SdkMeterProvider.builder().registerMetricReader(_metricReader).build();
        _openTelemetrySdk = OpenTelemetrySdk.builder().setMeterProvider(_metricProvider).buildAndRegisterGlobal();
    }

    @After
    public void tearDown() {
        _openTelemetrySdk.close();
    }

    @Test
    public void testHistograms() throws IOException {

        final Meter meter = _metricProvider.meterBuilder("mad-experimental").setSchemaUrl("mad").build();
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
        final MetricsRequestMarshaler marshaller = MetricsRequestMarshaler.create(_metricReader.collectAllMetrics());
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
        Assert.assertEquals(58d, statistics.get(new StatisticFactory().getStatistic("max")).get(0).getValue().getValue(), 0.01);
        Assert.assertEquals(4d, statistics.get(new StatisticFactory().getStatistic("count")).get(0).getValue().getValue(), 0.01);
        Assert.assertEquals(64d, statistics.get(new StatisticFactory().getStatistic("sum")).get(0).getValue().getValue(), 0.01);
    }

    @Test
    public void testExponentialHistograms() throws IOException {
        final SdkMeterProvider mp =
                SdkMeterProvider.builder().registerView(
                                InstrumentSelector.builder()
                                        .setName("my_histogram")
                                        .build(),
                                View.builder()
                                        .setAggregation(
                                                Aggregation.base2ExponentialBucketHistogram())
                                        .build())
                        .registerMetricReader(_metricReader).build();
        GlobalOpenTelemetry.resetForTest();
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
        final MetricsRequestMarshaler marshaller = MetricsRequestMarshaler.create(_metricReader.collectAllMetrics());
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
        Assert.assertEquals(58d, statistics.get(new StatisticFactory().getStatistic("max")).get(0).getValue().getValue(), 0.01);
        Assert.assertEquals(4d, statistics.get(new StatisticFactory().getStatistic("count")).get(0).getValue().getValue(), 0.01);
        Assert.assertEquals(64d, statistics.get(new StatisticFactory().getStatistic("sum")).get(0).getValue().getValue(), 0.01);
    }

    @Test
    public void testExponentialHistogramsLargeValue() throws IOException {
        final SdkMeterProvider mp =
                SdkMeterProvider.builder().registerView(
                                InstrumentSelector.builder()
                                        .setName("my_histogram")
                                        .build(),
                                View.builder()
                                        .setAggregation(
                                                Aggregation.base2ExponentialBucketHistogram())
                                        .build())
                        .registerMetricReader(_metricReader).build();
        GlobalOpenTelemetry.resetForTest();
        OpenTelemetrySdk.builder().setMeterProvider(mp).buildAndRegisterGlobal();

        final Meter meter = mp.meterBuilder("mad-experimental").setSchemaUrl("mad").build();
        final DoubleHistogram histo = meter.histogramBuilder("my_histogram").build();

        double val = 1.8e24;
        histo.record(val);
        histo.record(1);
        final MetricsRequestMarshaler marshaller = MetricsRequestMarshaler.create(_metricReader.collectAllMetrics());
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        marshaller.writeBinaryTo(stream);

        final OpenTelemetryGrpcRecordParser parser = new OpenTelemetryGrpcRecordParser();
        ExportMetricsServiceRequest request = MetricsService.Serializers.ExportMetricsServiceRequestSerializer.deserialize(ByteString.fromArray(stream.toByteArray()));
        final List<Record> records = parser.parseRecords(request);
        // Assert on records
        Assert.assertEquals(1, records.size());
        final Record record = records.get(0);
        final ImmutableMap<String, ? extends Metric> metrics = record.getMetrics();
        Assert.assertEquals(1, metrics.size());
        final Metric metric = metrics.get("my_histogram");
        Assert.assertNotNull(metric);
        final ImmutableMap<Statistic, ImmutableList<CalculatedValue<?>>> statistics = metric.getStatistics();
        Assert.assertEquals(1, statistics.get(new StatisticFactory().getStatistic("min")).get(0).getValue().getValue(), 0.01);
        Assert.assertEquals(val, statistics.get(new StatisticFactory().getStatistic("max")).get(0).getValue().getValue(), 0.01);
        Assert.assertEquals(2, statistics.get(new StatisticFactory().getStatistic("count")).get(0).getValue().getValue(), 0.01);
        Assert.assertEquals(val, statistics.get(new StatisticFactory().getStatistic("sum")).get(0).getValue().getValue(), 0.01);
    }
    @Test
    public void testBucketAndValueCalculations() {
        final List<Integer> scales = List.of(3, 2, 0, -2, -3);
        final List<Double> values = List.of(1.0, 2.0, 3.0, 58.0, 1.8e24, 1.8e240);
        for (int scale : scales) {
            final HistogramIndexer indexer = new HistogramIndexer(scale);
            for (double value : values) {
                final int index = indexer.getIndex(value);
                final double scaleFactor =  Math.scalb(LOG_BASE2_E, scale);
                final double returnedValue = OpenTelemetryGrpcRecordParser.mapIndexToValue(index, scale, scaleFactor);
                final double allowance = (Math.pow(2, Math.pow(2, -scale)) - 1) * value;
                Assert.assertEquals("Value " + value + " not equal to returned value " + returnedValue + " for scale " + scale + " and index " + index, value, returnedValue, allowance);
            }
        }
    }
}
