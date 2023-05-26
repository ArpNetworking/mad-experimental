/*
 * Copyright 2023 Inscope Metrics, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.arpnetworking.metrics.mad.experimental.sources;

import akka.util.ByteString;
import com.arpnetworking.metrics.mad.model.Metric;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.metrics.mad.model.statistics.HistogramStatistic;
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
import io.opentelemetry.proto.metrics.v1.ExponentialHistogram;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.View;
import io.opentelemetry.sdk.metrics.internal.aggregator.HistogramIndexer;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import it.unimi.dsi.fastutil.doubles.Double2LongMap;
import it.unimi.dsi.fastutil.objects.ObjectSortedSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Tests for the {@link OpenTelemetryGrpcRecordParser} class.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public class OpenTelemetryGrpcRecordParserTest {
    private InMemoryMetricReader _metricReader;
    private SdkMeterProvider _metricProvider;
    private static final double LOG_BASE2_E = 1D / Math.log(2);

    @Before
    public void setUp() {
        _metricReader = InMemoryMetricReader.createDelta();
        _metricProvider = SdkMeterProvider.builder().registerMetricReader(_metricReader).build();
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

        final OpenTelemetryGrpcRecordParser parser = new OpenTelemetryGrpcRecordParser();
        final List<Record> records = parser.parse(createRequest(_metricReader));
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

        final OpenTelemetryGrpcRecordParser parser = new OpenTelemetryGrpcRecordParser();
        final List<Record> records = parser.parse(createRequest(_metricReader));
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

        final Meter meter = mp.meterBuilder("mad-experimental").setSchemaUrl("mad").build();
        final DoubleHistogram histo = meter.histogramBuilder("my_histogram").build();

        final double val = 1.8e24;
        histo.record(val);
        histo.record(1);

        final OpenTelemetryGrpcRecordParser parser = new OpenTelemetryGrpcRecordParser();
        final List<Record> records = parser.parse(createRequest(_metricReader));
        // Assert on records
        Assert.assertEquals(1, records.size());
        final Record record = records.get(0);
        final ImmutableMap<String, ? extends Metric> metrics = record.getMetrics();
        Assert.assertEquals(1, metrics.size());
        final Metric metric = metrics.get("my_histogram");
        Assert.assertNotNull(metric);
        final ImmutableMap<Statistic, ImmutableList<CalculatedValue<?>>> statistics = metric.getStatistics();
        Assert.assertEquals(1, getStatisticValue(statistics, "min"), 0.01);
        Assert.assertEquals(val, getStatisticValue(statistics, "max"), 0.01);
        Assert.assertEquals(2, getStatisticValue(statistics, "count"), 0.01);
        Assert.assertEquals(val, getStatisticValue(statistics, "sum"), 0.01);
    }

    @Test
    @Ignore("Otel doesn't support negative values in histograms")
    public void testExponentialHistogramsNegativeValues() throws IOException {
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

        final Meter meter = mp.meterBuilder("mad-experimental").setSchemaUrl("mad").build();
        final DoubleHistogram histo = meter.histogramBuilder("my_histogram").build();


        histo.record(-50);
        histo.record(-100);
        histo.record(1);

        final OpenTelemetryGrpcRecordParser parser = new OpenTelemetryGrpcRecordParser();
        final List<Record> records = parser.parse(createRequest(_metricReader));
        // Assert on records
        Assert.assertEquals(1, records.size());
        final Record record = records.get(0);
        final ImmutableMap<String, ? extends Metric> metrics = record.getMetrics();
        Assert.assertEquals(1, metrics.size());
        final Metric metric = metrics.get("my_histogram");
        Assert.assertNotNull(metric);
        final ImmutableMap<Statistic, ImmutableList<CalculatedValue<?>>> statistics = metric.getStatistics();
        Assert.assertEquals(-100, getStatisticValue(statistics, "min"), 0.01);
        Assert.assertEquals(1, getStatisticValue(statistics, "max"), 0.01);
        Assert.assertEquals(3, getStatisticValue(statistics, "count"), 0.01);
        Assert.assertEquals(-149, getStatisticValue(statistics, "sum"), 0.01);
    }

    @Test
    public void testExponentialHistogramsZeroValues() throws IOException {
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

        final Meter meter = mp.meterBuilder("mad-experimental").setSchemaUrl("mad").build();
        final DoubleHistogram histo = meter.histogramBuilder("my_histogram").build();


        histo.record(0);
        histo.record(0);
        histo.record(1);

        final OpenTelemetryGrpcRecordParser parser = new OpenTelemetryGrpcRecordParser();
        final List<Record> records = parser.parse(createRequest(_metricReader));
        // Assert on records
        Assert.assertEquals(1, records.size());
        final Record record = records.get(0);
        final ImmutableMap<String, ? extends Metric> metrics = record.getMetrics();
        Assert.assertEquals(1, metrics.size());
        final Metric metric = metrics.get("my_histogram");
        Assert.assertNotNull(metric);
        final ImmutableMap<Statistic, ImmutableList<CalculatedValue<?>>> statistics = metric.getStatistics();
        Assert.assertEquals(0, getStatisticValue(statistics, "min"), 0.01);
        Assert.assertEquals(1, getStatisticValue(statistics, "max"), 0.01);
        Assert.assertEquals(3, getStatisticValue(statistics, "count"), 0.01);
        Assert.assertEquals(1, getStatisticValue(statistics, "sum"), 0.01);
    }

    @Test
    public void testExponentialHistogramsBuckets() throws IOException {
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

        final Meter meter = mp.meterBuilder("mad-experimental").setSchemaUrl("mad").build();
        final DoubleHistogram histo = meter.histogramBuilder("my_histogram").build();

        histo.record(0);
        histo.record(10000);
        histo.record(10000);
        histo.record(10000);
        histo.record(1.8e24);

        final OpenTelemetryGrpcRecordParser parser = new OpenTelemetryGrpcRecordParser();
        final ExportMetricsServiceRequest request = createRequest(_metricReader);
        final List<Record> records = parser.parse(request);
        // Assert on records
        Assert.assertEquals(1, records.size());
        final Record record = records.get(0);
        final ImmutableMap<String, ? extends Metric> metrics = record.getMetrics();
        Assert.assertEquals(1, metrics.size());
        final Metric metric = metrics.get("my_histogram");
        Assert.assertNotNull(metric);
        final ImmutableMap<Statistic, ImmutableList<CalculatedValue<?>>> statistics = metric.getStatistics();

        final ImmutableList<CalculatedValue<?>> histogram = getStatistic(statistics, "histogram");
        Assert.assertNotNull(histogram);
        Assert.assertEquals(1, histogram.size());
        final HistogramStatistic.HistogramSupportingData data = (HistogramStatistic.HistogramSupportingData) histogram.get(0).getData();
        Assert.assertNotNull(data);
        final HistogramStatistic.HistogramSnapshot histogramSnapshot = data.getHistogramSnapshot();
        final ObjectSortedSet<Double2LongMap.Entry> histogramValues = histogramSnapshot.getValues();
        Assert.assertEquals(3, histogramValues.size());

        final ExponentialHistogram otelHistogram = request
                .getResourceMetrics(0)
                .getScopeMetrics(0)
                .getMetrics(0)
                .getExponentialHistogram();
        final ExponentialHistogramDataPoint otelHistogramDataPoint = otelHistogram.getDataPoints(0);
        final HistogramIndexer indexer = new HistogramIndexer(otelHistogramDataPoint.getScale());
        for (Double2LongMap.Entry entry : histogramValues) {
            final double value = entry.getDoubleKey();
            if (Math.abs(value) < 0.001) {
                Assert.assertEquals(otelHistogramDataPoint.getZeroCount(), entry.getLongValue());
            } else if (value > 0) {
                final int index = indexer.getIndex(value) - otelHistogramDataPoint.getPositive().getOffset();
                final long count = otelHistogramDataPoint.getPositive().getBucketCounts(index);
                Assert.assertEquals(count, entry.getLongValue());
            } else {
                final int index = indexer.getIndex(value) - otelHistogramDataPoint.getNegative().getOffset();
                final long count = otelHistogramDataPoint.getNegative().getBucketCounts(index);
                Assert.assertEquals(count, entry.getLongValue());
            }
        }
    }

    @Test
    public void testBucketAndValueCalculations2() {
        final List<Double> values = List.of(8191.9999999999945, 8193.0, 8500.0, 10000.0, 1.8e24);
        final int scale = 1;
        final HistogramIndexer indexer = new HistogramIndexer(scale);
        for (double value : values) {
            final int index = indexer.getIndex(value);
            final double scaleFactor =  Math.scalb(LOG_BASE2_E, scale);
            final double returnedValue = OpenTelemetryGrpcRecordParser.mapIndexToValue(index, scale, scaleFactor);
            final double allowance = (Math.pow(2, Math.pow(2, -scale)) - 1) * value;
            final int newIndex = indexer.getIndex(returnedValue);
            Assert.assertEquals(
                    "Value %s not equal to returned value %s for scale %d and index %d".formatted(
                            value,
                            returnedValue,
                            scale,
                            index),
                    value,
                    returnedValue,
                    allowance);
        }
    }

    @Test
    public void testBucketAndValueCalculations3() {
        final List<Integer> scales = List.of(3, 2, 0, -2, -3);
        for (int scale: scales) {
            final HistogramIndexer indexer = new HistogramIndexer(scale);
            final Double base = Math.pow(2, Math.pow(2, -scale));
            for (int index = 0; index < 120; index++) {
                final double scaleFactor = Math.scalb(LOG_BASE2_E, scale);
                final double returnedValue = OpenTelemetryGrpcRecordParser.mapIndexToValue(index, scale, scaleFactor);
                final int newIndex = indexer.getIndex(returnedValue);
                Assert.assertEquals("Index %s not equal to otel returned index %s for scale %s, returned value %s".formatted(
                        index,
                        newIndex,
                        scale,
                        returnedValue), newIndex, index);
            }
        }
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
                Assert.assertEquals(
                        "Value %s not equal to returned value %s for scale %d and index %d".formatted(
                                value,
                                returnedValue,
                                scale,
                                index),
                        value,
                        returnedValue,
                        allowance);
            }
        }
    }

    private ExportMetricsServiceRequest createRequest(final InMemoryMetricReader reader) throws IOException {
        final MetricsRequestMarshaler marshaller = MetricsRequestMarshaler.create(reader.collectAllMetrics());
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        marshaller.writeBinaryTo(stream);
        return MetricsService.Serializers.ExportMetricsServiceRequestSerializer.deserialize(ByteString.fromArray(stream.toByteArray()));
    }

    private double getStatisticValue(
            final ImmutableMap<Statistic, ImmutableList<CalculatedValue<?>>> statistics,
            final String statistic) {
        final ImmutableList<CalculatedValue<?>> calculated = getStatistic(statistics, statistic);
        if (calculated == null) {
            throw new IllegalArgumentException("statistic not found: " + statistic);
        }
        return calculated.get(0).getValue().getValue();
    }

    @Nullable
    private ImmutableList<CalculatedValue<?>> getStatistic(
            final ImmutableMap<Statistic, ImmutableList<CalculatedValue<?>>> statistics,
            final String statistic) {
        return statistics.get(new StatisticFactory().getStatistic(statistic));

    }
}
