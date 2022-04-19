/*
 * Copyright 2021 Inscope Metrics, Inc.
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

import akka.actor.ActorSystem;
import akka.pattern.Patterns;
import com.arpnetworking.commons.builder.ThreadLocalBuilder;
import com.arpnetworking.metrics.mad.model.DefaultMetric;
import com.arpnetworking.metrics.mad.model.DefaultQuantity;
import com.arpnetworking.metrics.mad.model.DefaultRecord;
import com.arpnetworking.metrics.mad.model.MetricType;
import com.arpnetworking.metrics.mad.model.Quantity;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.metrics.mad.model.statistics.HistogramStatistic;
import com.arpnetworking.metrics.mad.model.statistics.Statistic;
import com.arpnetworking.metrics.mad.model.statistics.StatisticFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.CalculatedValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.MetricsService;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.InstrumentationLibraryMetrics;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.resource.v1.Resource;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

/**
 * Implementation of the GRPC Metrics service.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public class OpenTelemetryMetricsService implements MetricsService {

    /**
     * Public constructor.
     *
     * @param actorSystem {@link ActorSystem} the source is running in
     */
    public OpenTelemetryMetricsService(final ActorSystem actorSystem) {
        _actorSystem = actorSystem;
    }

    @Override
    public CompletionStage<ExportMetricsServiceResponse> export(final ExportMetricsServiceRequest in) {
        final Map<ImmutableMap<String, String>, Map<Long, Map<String, com.arpnetworking.metrics.mad.model.Metric>>> metricsMap =
                Maps.newHashMap();
        final List<ResourceMetrics> resourceMetrics = in.getResourceMetricsList();
        final List<Record> records = Lists.newArrayList();
        for (ResourceMetrics resourceMetric : resourceMetrics) {
            final Resource resource = resourceMetric.getResource();
            final ImmutableMap<String, String> resourceTags = getTags(resource.getAttributesList());

            final List<InstrumentationLibraryMetrics> libMetrics = resourceMetric.getInstrumentationLibraryMetricsList();
            for (InstrumentationLibraryMetrics libMetric : libMetrics) {
                final List<Metric> metrics = libMetric.getMetricsList();
                for (final Metric metric : metrics) {

                    // Here is the place where we have all the metadata to build a record
                    final String name = metric.getName();
                    switch (metric.getDataCase()) {
                        case SUM:
                            final List<NumberDataPoint> sumPointsList = metric.getSum().getDataPointsList();
                            convertNumberDataPoints(sumPointsList, name, resourceTags, metricsMap);

                            break;
                        case GAUGE:
                            final List<NumberDataPoint> pointsList = metric.getGauge().getDataPointsList();
                            convertNumberDataPoints(pointsList, name, resourceTags, metricsMap);

                            break;
                        case HISTOGRAM:
                            final List<HistogramDataPoint> histogramDataPoints = metric.getHistogram()
                                    .getDataPointsList();
                            convertHistogramDataPoints(histogramDataPoints, name, resourceTags, metricsMap);
                            break;
                        case EXPONENTIAL_HISTOGRAM:
                            final List<ExponentialHistogramDataPoint> expHistogramDataPoints = metric.getExponentialHistogram()
                                    .getDataPointsList();
                            convertExpHistogramDataPoints(expHistogramDataPoints, name, resourceTags, metricsMap);
                            break;
                        default:
                            RATE_LOGGER.warn()
                                    .setMessage("Unsupported data type")
                                    .addData("dataType", metric.getDataCase().name())
                                    .log();
                    }
                }
            }
        }

        metricsMap.forEach((k, v) -> {
            final ImmutableMap<String, String> dimensions = k;
            v.forEach((k2, v2) -> {
                final Long nanoTime = k2;
                final Record record = ThreadLocalBuilder.build(DefaultRecord.Builder.class, recordBuilder ->
                        recordBuilder.setId(UUID.randomUUID().toString())
                                .setDimensions(dimensions)
                                .setTime(ZonedDateTime.ofInstant(Instant.ofEpochMilli(nanoTime / 1_000_000), ZoneOffset.UTC))
                                .setMetrics(ImmutableMap.copyOf(v2)));
                records.add(record);
            });
        });


        return _actorSystem.actorSelection("/user/" + OpenTelemetryGrpcSource.ACTOR_NAME)
                .resolveOne(Duration.ofSeconds(1))
                .thenApply(a -> Patterns.ask(a, new OpenTelemetryGrpcSource.RecordsMessage(records), Duration.ofSeconds(3)))
                .thenApply(v -> ExportMetricsServiceResponse.newBuilder().build());
    }

    private static ImmutableMap<String, String> getTags(final List<KeyValue> attributesList) {
        boolean hasService = false;
        String serviceName = null;
        final ImmutableMap.Builder<String, String> tagsBuilder = ImmutableMap.builderWithExpectedSize(attributesList.size());
                for (KeyValue kv : attributesList) {
            if ("service.name".equals(kv.getKey())) {
                serviceName = kv.getValue().getStringValue();
            }
            if ("service".equals(kv.getKey())) {
                hasService = true;
            }
            tagsBuilder.put(kv.getKey(), kv.getValue().getStringValue());
        }

        if (!hasService && serviceName != null) {
            tagsBuilder.put("service", serviceName);
        }
        return tagsBuilder.build();
    }

    private static void convertNumberDataPoints(final List<NumberDataPoint> dataPoints, final String name,
                                                final ImmutableMap<String, String> resourceTags,
                                                final Map<ImmutableMap<String, String>,
                                                        Map<Long, Map<String,
                                                                com.arpnetworking.metrics.mad.model.Metric>>> metricsMap) {
        for (final NumberDataPoint point : dataPoints) {
            final double value;
            switch (point.getValueCase()) {
                case AS_INT:
                    value = point.getAsInt();
                    break;
                case AS_DOUBLE:
                    value = point.getAsDouble();
                    break;
                default:
                    value = 0;
                    break;
            }
            final Long timestamp = point.getTimeUnixNano();
            final Quantity quantity = ThreadLocalBuilder.build(DefaultQuantity.Builder.class, quantityBuilder -> {
                quantityBuilder.setValue(value);
            });
            final com.arpnetworking.metrics.mad.model.Metric madMetric = ThreadLocalBuilder.build(DefaultMetric.Builder.class, builder -> {
                builder.setValues(ImmutableList.of(quantity))
                        .setType(MetricType.GAUGE);
            });

            finalizeMetric(
                    name,
                    resourceTags,
                    metricsMap,
                    timestamp,
                    madMetric,
                    point.getAttributesCount(),
                    point.getAttributesList());
        }
    }

    private static void convertHistogramDataPoints(final List<HistogramDataPoint> dataPoints, final String name,
                                                   final ImmutableMap<String, String> resourceTags,
                                                   final Map<ImmutableMap<String, String>,
                                                           Map<Long, Map<String,
                                                                   com.arpnetworking.metrics.mad.model.Metric>>> metricsMap) {
        for (final HistogramDataPoint point : dataPoints) {
            if (point.getExplicitBoundsCount() == 0) {
                RATE_LOGGER.debug()
                        .setMessage("Discarding data")
                        .addData("reason", "no samples")
                        .addData("type", "histogram")
                        .log();
                continue;
            }

            final Map<Statistic, ImmutableList<CalculatedValue<?>>> statistics = Maps.newHashMap();
            final List<Map.Entry<Double, Long>> entries = Lists.newArrayList();

            Double lowEstimate = null;
            Double highEstimate = null;

            for (int x = 0; x < point.getExplicitBoundsCount(); x++) {
                final long count = point.getBucketCounts(x + 1);
                if (count > 0) {
                    entries.add(new AbstractMap.SimpleEntry<>(point.getExplicitBounds(x), count));
                    if (lowEstimate == null) {
                        lowEstimate = point.getExplicitBounds(x);
                    }
                    highEstimate = point.getExplicitBounds(x + 1);
                }
            }

            final double low = lowEstimate == null ? 0 : lowEstimate;
            final double high = highEstimate == null ? 0 : highEstimate;

            statistics.put(STATISTIC_FACTORY.getStatistic("min"), createCalculatedValue(low));
            statistics.put(STATISTIC_FACTORY.getStatistic("max"), createCalculatedValue(high));
            statistics.put(STATISTIC_FACTORY.getStatistic("count"), createCalculatedValue((double) point.getCount()));
            statistics.put(STATISTIC_FACTORY.getStatistic("sum"), createCalculatedValue(point.getSum()));

            statistics.put(
                    STATISTIC_FACTORY.getStatistic("histogram"),
                    createHistogramValue(entries));


            final Long timestamp = point.getTimeUnixNano();
            final com.arpnetworking.metrics.mad.model.Metric madMetric = ThreadLocalBuilder.build(DefaultMetric.Builder.class, builder -> {
                builder.setStatistics(ImmutableMap.copyOf(statistics))
                        .setType(MetricType.GAUGE);

            });

            finalizeMetric(
                    name,
                    resourceTags,
                    metricsMap,
                    timestamp,
                    madMetric,
                    point.getAttributesCount(),
                    point.getAttributesList());
        }
    }

    // CHECKSTYLE.OFF: ExecutableStatementCount - We need to repeat for positive and negative
    private static void convertExpHistogramDataPoints(final List<ExponentialHistogramDataPoint> dataPoints, final String name,
                                                      final ImmutableMap<String, String> resourceTags,
                                                      final Map<ImmutableMap<String, String>,
                                                        Map<Long, Map<String,
                                                                com.arpnetworking.metrics.mad.model.Metric>>> metricsMap) {
        for (final ExponentialHistogramDataPoint histogram : dataPoints) {
            final int scale = histogram.getScale() * -1;
            if (!(histogram.hasPositive() && histogram.getPositive().getBucketCountsCount() > 0)
                    || (histogram.hasNegative() && histogram.getNegative().getBucketCountsCount() > 0)) {
                RATE_LOGGER.debug()
                        .setMessage("Discarding data")
                        .addData("reason", "no samples")
                        .addData("type", "expHistogram")
                        .log();
                continue;
            }

            final Map<Statistic, ImmutableList<CalculatedValue<?>>> statistics = Maps.newHashMap();
            final List<Map.Entry<Double, Long>> entries = Lists.newArrayList();
            final ExponentialHistogramDataPoint.Buckets positive = histogram.getPositive();
            double lowEstimate = Double.POSITIVE_INFINITY;
            double highEstimate = Double.NEGATIVE_INFINITY;

            int offset = positive.getOffset();
            for (int x = 0; x < positive.getBucketCountsCount(); x++) {
                final int index = offset + x;
                //exponent := int64(index<<-e.scale) + ExponentBias
                // return math.Float64frombits(uint64(exponent << MantissaWidth))
                final long exponent = ((long) index << scale) + EXPONENT_BIAS;
                final double value = Double.longBitsToDouble(exponent << MANTISSA_WIDTH);
                final long count = positive.getBucketCounts(x);
                entries.add(new AbstractMap.SimpleEntry<>(value, count));
                if (value < lowEstimate) {
                    lowEstimate = value;
                }
                if (value > highEstimate) {
                    highEstimate = value;
                }
            }

            final ExponentialHistogramDataPoint.Buckets negative = histogram.getNegative();
            offset = negative.getOffset();
            for (int x = 0; x < negative.getBucketCountsCount(); x++) {
                final int index = offset + x;
                //exponent := int64(index<<-e.scale) + ExponentBias
                // return math.Float64frombits(uint64(exponent << MantissaWidth))
                final long exponent = ((long) index << scale) + EXPONENT_BIAS;
                final double value = Double.longBitsToDouble(exponent << MANTISSA_WIDTH | 1L << 63);
                final long count = negative.getBucketCounts(x);
                entries.add(new AbstractMap.SimpleEntry<>(value, count));
                if (value < lowEstimate) {
                    lowEstimate = value;
                }
                if (value > highEstimate) {
                    highEstimate = value;
                }
            }


            final double low = Double.isInfinite(lowEstimate) ? 0 : lowEstimate;
            final double high = Double.isInfinite(highEstimate) ? 0 : highEstimate;

            statistics.put(STATISTIC_FACTORY.getStatistic("min"), createCalculatedValue(low));
            statistics.put(STATISTIC_FACTORY.getStatistic("max"), createCalculatedValue(high));
            statistics.put(STATISTIC_FACTORY.getStatistic("count"), createCalculatedValue((double) histogram.getCount()));
            statistics.put(STATISTIC_FACTORY.getStatistic("sum"), createCalculatedValue(histogram.getSum()));

            statistics.put(
                    STATISTIC_FACTORY.getStatistic("histogram"),
                    createHistogramValue(entries));


            final Long timestamp = histogram.getTimeUnixNano();
            final com.arpnetworking.metrics.mad.model.Metric madMetric = ThreadLocalBuilder.build(DefaultMetric.Builder.class, builder -> {
                builder.setStatistics(ImmutableMap.copyOf(statistics))
                        .setType(MetricType.GAUGE);

            });

            finalizeMetric(
                    name,
                    resourceTags,
                    metricsMap,
                    timestamp,
                    madMetric,
                    histogram.getAttributesCount(),
                    histogram.getAttributesList());
        }
    }
    // CHECKSTYLE.ON: ExecutableStatementCount

    private static ImmutableList<CalculatedValue<?>> createHistogramValue(final List<Map.Entry<Double, Long>> entries) {
        return ImmutableList.of(
                // CHECKSTYLE.OFF: LineLength - Generic specification required for buildGeneric
                ThreadLocalBuilder.<CalculatedValue<HistogramStatistic.HistogramSupportingData>, CalculatedValue.Builder<HistogramStatistic.HistogramSupportingData>>buildGeneric(
                // CHECKSTYLE.ON: LineLength
                CalculatedValue.Builder.class,
                b1 -> b1.setValue(
                                ThreadLocalBuilder.build(
                                        DefaultQuantity.Builder.class,
                                        b2 -> b2.setValue(1.0)))
                        .setData(
                                ThreadLocalBuilder.build(
                                        HistogramStatistic.HistogramSupportingData.Builder.class,
                                        b3 -> {
                                            final HistogramStatistic.Histogram histogram =
                                                    new HistogramStatistic.Histogram();
                                            entries.forEach(
                                                    e -> histogram.recordValue(
                                                            e.getKey(),
                                                            e.getValue()));
                                            b3.setHistogramSnapshot(histogram.getSnapshot());
                                        }))));
    }

    private static ImmutableList<CalculatedValue<?>> createCalculatedValue(final double low) {
        return ImmutableList.of(ThreadLocalBuilder.<CalculatedValue<Void>, CalculatedValue.Builder<Void>>buildGeneric(
                CalculatedValue.Builder.class,
                b1 -> b1.setValue(
                        ThreadLocalBuilder.build(
                                DefaultQuantity.Builder.class,
                                b2 -> b2.setValue(low)))));
    }

    // CHECKSTYLE.OFF: LineLength - Crazy map types
    private static void finalizeMetric(final String name,
                                       final ImmutableMap<String, String> resourceTags,
                                       final Map<ImmutableMap<String, String>, Map<Long, Map<String, com.arpnetworking.metrics.mad.model.Metric>>> metricsMap,
                                       final Long timestamp,
                                       final com.arpnetworking.metrics.mad.model.Metric madMetric,
                                       final int attributesCount,
                                       final List<KeyValue> attributesList) {
        // CHECKSTYLE.ON: LineLength
        final ImmutableMap<String, String> finalTags;
        if (attributesCount > 0) {
            final Map<String, String> tags = Maps.newHashMapWithExpectedSize(attributesCount + resourceTags.size());
            for (KeyValue kv : attributesList) {
                final String key = kv.getKey();
                String value = kv.getValue().getStringValue();

                if ("".equals(value)) {
                    value = "[empty]";
                }

                if (tags.put(key, value) != null) {
                    throw new IllegalStateException("Duplicate key");
                }
            }
            for (Map.Entry<String, String> entry : resourceTags.entrySet()) {
                final String key = entry.getKey();
                String value = entry.getValue();

                if ("".equals(value)) {
                    value = "[empty]";
                }
                tags.putIfAbsent(key, value);
            }
            finalTags = ImmutableMap.copyOf(tags);
        } else {
            finalTags = resourceTags;
        }

        metricsMap.compute(finalTags, (k, v) -> {
            final Map<Long, Map<String, com.arpnetworking.metrics.mad.model.Metric>> m1;
            if (v == null) {
                m1 = Maps.newHashMap();
            } else {
                m1 = v;
            }
            m1.compute(timestamp, (k2, v2) -> {
                final Map<String, com.arpnetworking.metrics.mad.model.Metric> m2;
                if (v2 == null) {
                    m2 = Maps.newHashMap();
                } else {
                    m2 = v2;
                }
                m2.put(name, madMetric);
                return m2;
            });
            return m1;
        });
    }

    private final ActorSystem _actorSystem;
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenTelemetryMetricsService.class);
    private static final Logger RATE_LOGGER = LoggerFactory.getRateLimitLogger(OpenTelemetryMetricsService.class, Duration.ofSeconds(30));
    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final long EXPONENT_BIAS = (1 << 10) - 1;
    private static final long MANTISSA_WIDTH = 52;
}
