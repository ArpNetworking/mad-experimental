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
import com.arpnetworking.metrics.mad.model.DefaultMetric;
import com.arpnetworking.metrics.mad.model.DefaultQuantity;
import com.arpnetworking.metrics.mad.model.DefaultRecord;
import com.arpnetworking.metrics.mad.model.Metric;
import com.arpnetworking.metrics.mad.model.MetricType;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.metrics.mad.model.Unit;
import com.arpnetworking.metrics.mad.model.statistics.HistogramStatistic;
import com.arpnetworking.metrics.mad.model.statistics.Statistic;
import com.arpnetworking.metrics.mad.model.statistics.StatisticFactory;
import com.arpnetworking.tsdcore.model.CalculatedValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
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
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;


/**
 * Tests for the {@link OpenTelemetryGrpcRecordParser} class.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public class OpenTelemetryGrpcRecordParserTest {
    private InMemoryMetricReader _metricReader;
    private SdkMeterProvider _metricProvider;

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
        histo.record(0.0, attrs);
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
        Assert.assertEquals(58d, getStatisticValue(statistics, "max"), 0.01);
        Assert.assertEquals(5d, getStatisticValue(statistics, "count"), 0.01);
        Assert.assertEquals(64d, getStatisticValue(statistics, "sum"), 0.01);
        final CalculatedValue<?> histogramValue = getStatistic(statistics, "histogram").get(0);
        final HistogramStatistic.HistogramSupportingData data = (HistogramStatistic.HistogramSupportingData) histogramValue.getData();
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
        Assert.assertEquals(58d, getStatisticValue(statistics, "max"), 0.01);
        Assert.assertEquals(4d, getStatisticValue(statistics, "count"), 0.01);
        Assert.assertEquals(64d, getStatisticValue(statistics, "sum"), 0.01);
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
        final IndexToValue indexToValue = IndexToValueFactory.create(scale);
        final HistogramIndexer indexer = new HistogramIndexer(scale);
        for (double value : values) {
            final int index = indexer.getIndex(value);
            final double returnedValue = indexToValue.map(index);
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
    public void testBase2IndexToValue() {
        final List<Integer> scales = List.of(3, 2, 0, -2, -3);
        for (int scale: scales) {
            final IndexToValue indexToValue = IndexToValueFactory.create(scale);
            final IndexToValue base2IndexToValue = new Base2IndexToValue(scale);
            for (int index = -120; index < 120; index++) {
                final double expected = indexToValue.map(index);
                final double value = base2IndexToValue.map(index);
                Assert.assertEquals("Value %s not equal to returned value %s for scale %d and index %d".formatted(
                        expected,
                        value,
                        scale,
                        index), expected, value, 0.00001);
            }
        }
    }

    @Test
    public void testBucketAndValueCalculations3() {
        final List<Integer> scales = List.of(3, 2, 0, -2, -3);
        for (int scale: scales) {
            final HistogramIndexer indexer = new HistogramIndexer(scale);
            final IndexToValue indexToValue = IndexToValueFactory.create(scale);
            for (int index = 0; index < 120; index++) {
                final double returnedValue = indexToValue.map(index);
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
            final IndexToValue indexToValue = IndexToValueFactory.create(scale);
            for (double value : values) {
                final int index = indexer.getIndex(value);
                final double returnedValue = indexToValue.map(index);
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

    // CHECKSTYLE.OFF: MethodLength - Huge expected object
    @Test
    public void testRegression1() throws IOException {
        final byte[] tmp = Resources.toByteArray(Resources.getResource(OpenTelemetryGrpcRecordParserTest.class, "grpcrequest1.bin"));
        final byte[] framedRequestBytes = Arrays.copyOfRange(tmp, 5, tmp.length);

        final ExportMetricsServiceRequest request = MetricsService.Serializers.ExportMetricsServiceRequestSerializer
                .deserialize(ByteString.fromArray(framedRequestBytes));
        final OpenTelemetryGrpcRecordParser parser = new OpenTelemetryGrpcRecordParser();
        final StatisticFactory sf = new StatisticFactory();
        final List<Record> expectedRecords = List.of(
                new DefaultRecord.Builder()
                        .setMetrics(ImmutableMap.of(
                                "http.server.active_requests",
                                new DefaultMetric.Builder().setType(MetricType.GAUGE).setValues(
                                        ImmutableList.of(new DefaultQuantity.Builder().setValue(0d).build())).build()))
                        .setId("abc")
                        .setDimensions(ImmutableMap.of(
                                "http.server_name", "central-services.api.staging.topsort.ai",
                                "service.name", "central-services",
                                "http.flavor", "1.1",
                                "service", "central-services",
                                "http.method", "POST",
                                "http.scheme", "http",
                                "http.host", "10.1.23.61:5000"))
                        .setTime(ZonedDateTime.parse("2023-05-31T20:15:55.071Z"))
                        .build(),
                new DefaultRecord.Builder()
                        .setMetrics(ImmutableMap.of(
                                "runtime.python.asyncio.task_lag",
                                new DefaultMetric.Builder()
                                        .setType(MetricType.GAUGE)
                                        .setValues(ImmutableList.of())
                                        .setStatistics(
                                                ImmutableMap.of(
                                                        sf.getStatistic("max"),
                                                        ImmutableList.of(
                                                                new CalculatedValue.Builder<Double>()
                                                                        .setValue(
                                                                                new DefaultQuantity.Builder()
                                                                                        .setValue(6.984919587171845E-11)
                                                                                        .setUnit(Unit.SECOND)
                                                                                        .build())
                                                                        .build()),
                                                        sf.getStatistic("min"),
                                                        ImmutableList.of(
                                                                new CalculatedValue.Builder<Double>()
                                                                        .setValue(
                                                                                new DefaultQuantity.Builder()
                                                                                        .setValue(6.984919587171845E-11)
                                                                                        .setUnit(Unit.SECOND)
                                                                                        .build())
                                                                        .build()),
                                                        sf.getStatistic("sum"),
                                                        ImmutableList.of(
                                                                new CalculatedValue.Builder<Double>()
                                                                        .setValue(
                                                                                new DefaultQuantity.Builder()
                                                                                        .setValue(6.984919587171845E-11)
                                                                                        .setUnit(Unit.SECOND)
                                                                                        .build())
                                                                        .build()),
                                                        sf.getStatistic("count"),
                                                        ImmutableList.of(
                                                                new CalculatedValue.Builder<Double>()
                                                                        .setValue(
                                                                                new DefaultQuantity.Builder()
                                                                                        .setValue(1d)
                                                                                        .build())
                                                                        .build()),
                                                        sf.getStatistic("histogram"),
                                                        ImmutableList.of(
                                                                new CalculatedValue.Builder<HistogramStatistic.HistogramSupportingData>()
                                                                        .setValue(
                                                                                new DefaultQuantity.Builder()
                                                                                        .setValue(1d)
                                                                                        .build())
                                                                        .setData(
                                                                                new HistogramStatistic.HistogramSupportingData.Builder()
                                                                                        .setHistogramSnapshot(histogramSnapshotOf(6.95E-8))
                                                                                        .build())
                                                                        .build())))
                                        .build(),
                                "runtime.python.asyncio.active_tasks",
                                new DefaultMetric.Builder()
                                        .setType(MetricType.GAUGE)
                                        .setValues(
                                                ImmutableList.of(
                                                        new DefaultQuantity.Builder()
                                                                .setValue(3d)
                                                                .build()))
                                        .build()))
                        .setId("abc")
                        .setDimensions(
                                ImmutableMap.of(
                                        "service.name", "central-services",
                                        "service", "central-services"))
                        .setTime(ZonedDateTime.parse("2023-05-31T20:15:55.071Z"))
                        .build(),
                new DefaultRecord.Builder()
                        .setMetrics(
                                ImmutableMap.of(
                                        "http.server.active_requests",
                                        new DefaultMetric.Builder()
                                                .setType(MetricType.GAUGE)
                                                .setValues(
                                                        ImmutableList.of(
                                                                new DefaultQuantity.Builder()
                                                                        .setValue(0d)
                                                                        .build()))
                                                .build()))
                        .setId("abc")
                        .setDimensions(
                                ImmutableMap.of(
                                        "http.server_name", "central-services.api.staging.topsort.ai",
                                        "service.name", "central-services",
                                        "http.flavor", "1.1",
                                        "service", "central-services",
                                        "http.method", "GET",
                                        "http.scheme", "http",
                                        "http.host", "10.1.23.61:5000"))
                        .setTime(ZonedDateTime.parse("2023-05-31T20:15:55.071Z"))
                        .build(),
                new DefaultRecord.Builder()
                        .setMetrics(ImmutableMap.of(
                                "http.server.duration",
                                new DefaultMetric.Builder().setType(MetricType.GAUGE).setValues(ImmutableList.of()).setStatistics(
                                                ImmutableMap.of(
                                                        sf.getStatistic("max"),
                                                        ImmutableList.of(
                                                                new CalculatedValue.Builder<Double>()
                                                                        .setValue(
                                                                                new DefaultQuantity.Builder()
                                                                                        .setValue(0.002d)
                                                                                        .setUnit(Unit.SECOND)
                                                                                        .build())
                                                                        .build()),
                                                        sf.getStatistic("min"),
                                                        ImmutableList.of(
                                                                new CalculatedValue.Builder<Double>()
                                                                        .setValue(
                                                                                new DefaultQuantity.Builder()
                                                                                        .setValue(2d)
                                                                                        .setUnit(Unit.MILLISECOND)
                                                                                        .build())
                                                                        .build()),
                                                        sf.getStatistic("sum"),
                                                        ImmutableList.of(
                                                                new CalculatedValue.Builder<Double>()
                                                                        .setValue(
                                                                                new DefaultQuantity.Builder()
                                                                                        .setValue(0.002d)
                                                                                        .setUnit(Unit.SECOND)
                                                                                        .build())
                                                                        .build()),
                                                        sf.getStatistic("count"),
                                                        ImmutableList.of(
                                                                new CalculatedValue.Builder<Double>()
                                                                        .setValue(
                                                                                new DefaultQuantity.Builder()
                                                                                        .setValue(1d)
                                                                                        .build())
                                                                        .build()),
                                                        sf.getStatistic("histogram"),
                                                        ImmutableList.of(
                                                                new CalculatedValue.Builder<HistogramStatistic.HistogramSupportingData>()
                                                                        .setValue(
                                                                                new DefaultQuantity.Builder()
                                                                                        .setValue(1d).build())
                                                                        .setData(
                                                                                new HistogramStatistic.HistogramSupportingData.Builder()
                                                                                        .setHistogramSnapshot(histogramSnapshotOf(1.999d))
                                                                                        .build())
                                                                        .build())
                                                )
                                        ).build()))
                        .setId("abc")
                        .setDimensions(ImmutableMap.of(
                                "http.server_name", "central-services.api.staging.topsort.ai",
                                "service.name", "central-services",
                                "http.target", "/",
                                "http.flavor", "1.1",
                                "service", "central-services",
                                "net.host.port", "5000",
                                "http.method", "GET",
                                "http.scheme", "http",
                                "http.host", "10.1.23.61:5000"))
                        .setTime(ZonedDateTime.parse("2023-05-31T20:15:55.071Z"))
                        .build(),
                new DefaultRecord.Builder()
                        .setMetrics(ImmutableMap.of(
                                "http.server.active_requests",
                                new DefaultMetric.Builder()
                                        .setType(MetricType.GAUGE)
                                        .setValues(
                                                ImmutableList.of(
                                                        new DefaultQuantity.Builder()
                                                                .setValue(0d)
                                                                .build()))
                                        .build()))
                        .setId("abc")
                        .setDimensions(ImmutableMap.of(
                                "http.server_name", "central-services",
                                "service.name", "central-services",
                                "http.flavor", "1.1",
                                "service", "central-services",
                                "http.method", "GET",
                                "http.scheme", "http",
                                "http.host", "10.1.23.61:5000"))
                        .setTime(ZonedDateTime.parse("2023-05-31T20:15:55.071Z"))
                        .build()
        );
        final List<Record> records = parser.parse(request);
        assertRecords(expectedRecords, records);
    }
    // CHECKSTYLE.ON: MethodLength

    private void assertRecords(final List<Record> expected, final List<Record> actual) {
        Assert.assertEquals("Expected and actual records differ in length", expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertRecord(expected.get(i), actual.get(i));
        }
    }

    private void assertRecord(final Record expected, final Record actual) {
        Assert.assertEquals("Annotations do not match", expected.getAnnotations(), actual.getAnnotations());
        Assert.assertEquals("Dimensions do not match", expected.getDimensions(), actual.getDimensions());
//        Assert.assertEquals(expected.getId(), actual.getId());
        Assert.assertEquals("Time does not match", expected.getTime(), actual.getTime());
        Assert.assertEquals("Request time does not match", expected.getRequestTime(), actual.getRequestTime());
        assertMetrics(expected.getMetrics(), actual.getMetrics());
    }

    private void assertMetrics(final ImmutableMap<String, ? extends Metric> expected, final ImmutableMap<String, ? extends Metric> actual) {
        Assert.assertEquals(expected.size(), actual.size());

        for (Map.Entry<String, ? extends Metric> entry : expected.entrySet()) {
            final String key = entry.getKey();
            final Metric expectedMetric = entry.getValue();
            final Metric actualMetric = actual.get(key);
            Assert.assertNotNull("Did not find expected metric named %s".formatted(key), actualMetric);
            assertMetric(expectedMetric, actualMetric);

        }
    }

    private void assertMetric(final Metric expectedMetric, final Metric actualMetric) {
        Assert.assertEquals(expectedMetric.getType(), actualMetric.getType());
        Assert.assertEquals(expectedMetric.getValues(), actualMetric.getValues());
        assertStatistics(expectedMetric.getStatistics(), actualMetric.getStatistics());
    }

    private void assertStatistics(
            final ImmutableMap<Statistic, ImmutableList<CalculatedValue<?>>> expected,
            final ImmutableMap<Statistic, ImmutableList<CalculatedValue<?>>> actual) {
        Assert.assertEquals(expected.size(), actual.size());
        for (Map.Entry<Statistic, ImmutableList<CalculatedValue<?>>> entry : expected.entrySet()) {
            final Statistic key = entry.getKey();
            final ImmutableList<CalculatedValue<?>> expectedValue = entry.getValue();
            final ImmutableList<CalculatedValue<?>> actualValue = actual.get(key);
            Assert.assertNotNull("Did not find expected statistic named %s".formatted(key.getName()), actualValue);
            assertStatistic(expectedValue, actualValue);
        }
    }

    private void assertStatistic(final ImmutableList<CalculatedValue<?>> expected, final ImmutableList<CalculatedValue<?>> actual) {
        Assert.assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertCalculatedValue(expected.get(i), actual.get(i));
        }
    }

    private void assertCalculatedValue(final CalculatedValue<?> expected, final CalculatedValue<?> actual) {
        Assert.assertEquals(expected.getValue(), actual.getValue());
        final Object expectedData = expected.getData();
        final Object actualData = actual.getData();
        if (expectedData != null && actualData != null) {
            Assert.assertEquals(expectedData.getClass(), actualData.getClass());

            if (expectedData.getClass().equals(HistogramStatistic.HistogramSupportingData.class)) {
                final HistogramStatistic.HistogramSupportingData expectedHisto = (HistogramStatistic.HistogramSupportingData) expectedData;
                final HistogramStatistic.HistogramSupportingData actualHisto = (HistogramStatistic.HistogramSupportingData) actualData;
                Assert.assertEquals(expectedHisto.getHistogramSnapshot().getValues(), actualHisto.getHistogramSnapshot().getValues());
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

    private HistogramStatistic.HistogramSnapshot histogramSnapshotOf(final double... values) {
        final HistogramStatistic.Histogram histogram = new HistogramStatistic.Histogram();
        for (double value : values) {
            histogram.recordValue(value);
        }
        return histogram.getSnapshot();
    }

    @Nullable
    private ImmutableList<CalculatedValue<?>> getStatistic(
            final ImmutableMap<Statistic, ImmutableList<CalculatedValue<?>>> statistics,
            final String statistic) {
        return statistics.get(new StatisticFactory().getStatistic(statistic));

    }
}
