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

import com.arpnetworking.commons.builder.ThreadLocalBuilder;
import com.arpnetworking.metrics.common.parsers.Parser;
import com.arpnetworking.metrics.mad.model.DefaultMetric;
import com.arpnetworking.metrics.mad.model.DefaultQuantity;
import com.arpnetworking.metrics.mad.model.DefaultRecord;
import com.arpnetworking.metrics.mad.model.Metric;
import com.arpnetworking.metrics.mad.model.MetricType;
import com.arpnetworking.metrics.mad.model.Quantity;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.metrics.mad.model.Unit;
import com.arpnetworking.metrics.mad.model.statistics.HistogramStatistic;
import com.arpnetworking.metrics.mad.model.statistics.Statistic;
import com.arpnetworking.metrics.mad.model.statistics.StatisticFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.CalculatedValue;
import com.google.common.base.Functions;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.resource.v1.Resource;
import org.apache.pekko.http.scaladsl.model.ParsingException;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Parses the OTel protobuf binary protocol into records.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public class OpenTelemetryGrpcRecordParser implements Parser<List<Record>, ExportMetricsServiceRequest> {
    /**
     * Public constructor.
     */
    public OpenTelemetryGrpcRecordParser() {
        this(true);
    }

    /**
     * Public constructor.
     *
     * @param useCache whether to use a cache for the parser
     */
    public OpenTelemetryGrpcRecordParser(final boolean useCache) {
        if (useCache) {
            final Function<Integer, IndexToValue> indexToValueFromFactory = IndexToValueFactory::create;
            _indexToValueFactory = indexToValueFromFactory;
        } else {
            final LoadingCache<Integer, IndexToValue> indexToValueCache = CacheBuilder.newBuilder()
                    .maximumSize(100)
                    .build(CacheLoader.from(IndexToValueFactory::create));
            _indexToValueFactory = indexToValueCache::getUnchecked;
        }
    }

    @Override
    public List<Record> parse(final ExportMetricsServiceRequest request) throws ParsingException {
        final Map<ImmutableMap<String, String>, Map<Long, Map<String, Metric>>> metricsMap =
                Maps.newHashMap();
        final List<ResourceMetrics> resourceMetrics = request.getResourceMetricsList();
        final List<Record> records = Lists.newArrayList();
        for (ResourceMetrics resourceMetric : resourceMetrics) {
            final Resource resource = resourceMetric.getResource();
            final ImmutableMap<String, String> resourceTags = getTags(resource.getAttributesList());

            final List<ScopeMetrics> scopeMetrics = resourceMetric.getScopeMetricsList();
            for (ScopeMetrics scopeMetric : scopeMetrics) {
                final List<io.opentelemetry.proto.metrics.v1.Metric> metrics = scopeMetric.getMetricsList();
                for (final io.opentelemetry.proto.metrics.v1.Metric metric : metrics) {

                    // Here is the place where we have all the metadata to build a record
                    final String name = metric.getName();
                    final Unit unit = Optional.of(metric.getUnit()).map(String::toLowerCase).map(UNITS_MAP::get).orElse(null);
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
                            convertHistogramDataPoints(histogramDataPoints, name, resourceTags, metricsMap, unit);
                            break;
                        case EXPONENTIAL_HISTOGRAM:
                            final List<ExponentialHistogramDataPoint> expHistogramDataPoints = metric.getExponentialHistogram()
                                    .getDataPointsList();
                            convertExpHistogramDataPoints(expHistogramDataPoints, name, resourceTags, metricsMap, unit);
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
        return records;
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

            final String strVal = anyvalToString(kv.getValue());
            if (!Strings.isNullOrEmpty(strVal)) {
                tagsBuilder.put(kv.getKey(), strVal);
            }
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
            if (!Double.isFinite(value)) {
                RATE_LOGGER.warn()
                        .setMessage("Invalid value")
                        .addData("value", value)
                        .log();
                continue;
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
    // CHECKSTYLE.OFF: ExecutableStatementCount - It's complicated
    // CHECKSTYLE.OFF: MethodLength - It's complicated
    private static void convertHistogramDataPoints(final List<HistogramDataPoint> dataPoints, final String name,
                                                   final ImmutableMap<String, String> resourceTags,
                                                   final Map<ImmutableMap<String, String>,
                                                           Map<Long, Map<String,
                                                                   Metric>>> metricsMap, @Nullable final Unit unit) {
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
                final long count = point.getBucketCounts(x);
                if (count > 0) {
                    entries.add(new AbstractMap.SimpleEntry<>(point.getExplicitBounds(x), count));
                    if (lowEstimate == null) {
                        lowEstimate = point.getExplicitBounds(x);
                    }
                    if (x < point.getExplicitBoundsCount() - 1) {
                        highEstimate = point.getExplicitBounds(x + 1);
                    }
                }
            }

            // Last bucket, (last boundary, +inf)
            // We use the value of the max if it exists, otherwise we use the last boundary + the second to last boundary
            final long lastBucketCount = point.getBucketCounts(point.getExplicitBoundsCount());
            if (lastBucketCount > 0) {
                final double lastBucketValue;
                if (point.hasMax()) {
                    lastBucketValue = point.getMax();
                } else {
                    final double lastBound = point.getExplicitBounds(point.getExplicitBoundsCount() - 1);
                    final double secondLastBound = point.getExplicitBounds(point.getExplicitBoundsCount() - 2);
                    lastBucketValue = lastBound + Math.abs(lastBound - secondLastBound);
                }
                entries.add(new AbstractMap.SimpleEntry<>(lastBucketValue, lastBucketCount));
            }

            final double low;
            final double high;

            if (point.hasMin()) {
                low = point.getMin();
            } else {
                low = lowEstimate == null ? 0 : lowEstimate;
            }

            if (point.hasMax()) {
                high = point.getMax();
            } else {
                high = highEstimate == null ? 0 : highEstimate;
            }

            statistics.put(STATISTIC_FACTORY.getStatistic("min"), createCalculatedValue(low, unit));
            statistics.put(STATISTIC_FACTORY.getStatistic("max"), createCalculatedValue(high, unit));
            statistics.put(STATISTIC_FACTORY.getStatistic("count"), createCalculatedValue((double) point.getCount(), null));
            statistics.put(STATISTIC_FACTORY.getStatistic("sum"), createCalculatedValue(point.getSum(), unit));

            statistics.put(
                    STATISTIC_FACTORY.getStatistic("histogram"),
                    createHistogramValue(entries, unit));


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
    // CHECKSTYLE.ON: MethodLength
    // CHECKSTYLE.ON: ExecutableStatementCount

    // CHECKSTYLE.OFF: ExecutableStatementCount - We need to repeat for positive and negative
    // CHECKSTYLE.OFF: MethodLength - We need to repeat for positive and negative
    // CHECKSTYLE.OFF: UnnecessaryParentheses - Want to be very clear about order of operations
    private void convertExpHistogramDataPoints(final List<ExponentialHistogramDataPoint> dataPoints, final String name,
                                               final ImmutableMap<String, String> resourceTags,
                                               final Map<ImmutableMap<String, String>,
                                                              Map<Long, Map<String,
                                                                      Metric>>> metricsMap,
                                               @Nullable final Unit unit) {
        for (final ExponentialHistogramDataPoint histogram : dataPoints) {
            final int scale = histogram.getScale();

            if (!((histogram.hasPositive() && histogram.getPositive().getBucketCountsCount() > 0)
                    || (histogram.hasNegative() && histogram.getNegative().getBucketCountsCount() > 0)
                    || histogram.getZeroCount() > 0)) {
                RATE_LOGGER.debug()
                        .setMessage("Discarding data")
                        .addData("reason", "no samples")
                        .addData("type", "expHistogram")
                        .log();
                continue;
            }

            final Map<Statistic, ImmutableList<CalculatedValue<?>>> statistics = Maps.newHashMap();
            final List<Map.Entry<Double, Long>> entries = Lists.newArrayList();

            double lowEstimate = Double.POSITIVE_INFINITY;
            double highEstimate = Double.NEGATIVE_INFINITY;
            final IndexToValue indexToValue;
            indexToValue = _indexToValueFactory.apply(scale);  // This should never throw
            final ExponentialHistogramDataPoint.Buckets positive = histogram.getPositive();
            int offset = positive.getOffset();
            for (int x = 0; x < positive.getBucketCountsCount(); x++) {
                final long count = positive.getBucketCounts(x);
                if (count == 0) {
                    continue;
                }
                final int index = offset + x;

                final double value = indexToValue.map(index);

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
                final long count = negative.getBucketCounts(x);
                if (count == 0) {
                    continue;
                }
                final int index = offset + x;
                final double value =  -indexToValue.map(index);

                entries.add(new AbstractMap.SimpleEntry<>(value, count));
                if (value < lowEstimate) {
                    lowEstimate = value;
                }
                if (value > highEstimate) {
                    highEstimate = value;
                }
            }

            if (histogram.getZeroCount() > 0) {
                entries.add(new AbstractMap.SimpleEntry<>(0d, histogram.getZeroCount()));
            }

            final double low;
            if (histogram.hasMin()) {
                low = histogram.getMin();
            } else {
                low = Double.isInfinite(lowEstimate) ? 0 : lowEstimate;
            }

            final double high;
            if (histogram.hasMax()) {
                high = histogram.getMax();
            } else {
                high = Double.isInfinite(highEstimate) ? 0 : highEstimate;
            }

            statistics.put(STATISTIC_FACTORY.getStatistic("min"), createCalculatedValue(low, unit));
            statistics.put(STATISTIC_FACTORY.getStatistic("max"), createCalculatedValue(high, unit));
            statistics.put(STATISTIC_FACTORY.getStatistic("count"), createCalculatedValue((double) histogram.getCount(), null));
            statistics.put(STATISTIC_FACTORY.getStatistic("sum"), createCalculatedValue(histogram.getSum(), unit));

            statistics.put(
                    STATISTIC_FACTORY.getStatistic("histogram"),
                    createHistogramValue(entries, unit));


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


    // CHECKSTYLE.ON: UnnecessaryParentheses
    // CHECKSTYLE.ON: MethodLength
    // CHECKSTYLE.ON: ExecutableStatementCount

    private static ImmutableList<CalculatedValue<?>> createHistogramValue(
            final List<Map.Entry<Double, Long>> entries,
            @Nullable final Unit unit) {

        final Function<Double, Double> unitMapFunction =
                unit != null ? (Double v) -> unit.getType().getDefaultUnit().convert(v, unit)
                        : Functions.identity();
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
                                                                    unitMapFunction.apply(e.getKey()),
                                                                    e.getValue()));
                                                    b3.setHistogramSnapshot(histogram.getSnapshot());
                                                }))));
    }

    private static ImmutableList<CalculatedValue<?>> createCalculatedValue(final double low, @Nullable final Unit unit) {
        return ImmutableList.of(ThreadLocalBuilder.<CalculatedValue<Void>, CalculatedValue.Builder<Void>>buildGeneric(
                CalculatedValue.Builder.class,
                b1 -> b1.setValue(
                        ThreadLocalBuilder.build(
                                DefaultQuantity.Builder.class,
                                b2 -> b2.setValue(low)
                                        .setUnit(unit)))));
    }

    private static String anyvalToString(final AnyValue value) {
        String stringValue = "";
        switch (value.getValueCase()) {
            case STRING_VALUE:
                stringValue = value.getStringValue();

                if (Strings.isNullOrEmpty(stringValue)) {
                    stringValue = "";
                }
                break;
            case BOOL_VALUE:
                stringValue = Boolean.toString(value.getBoolValue());
                break;
            case INT_VALUE:
                stringValue = Long.toString(value.getIntValue());
                break;
            case DOUBLE_VALUE:
                stringValue = Double.toString(value.getDoubleValue());
                break;
            case ARRAY_VALUE:
                stringValue = value.getArrayValue()
                        .getValuesList()
                        .stream()
                        .map(OpenTelemetryGrpcRecordParser::anyvalToString)
                        .collect(Collectors.joining(","));
                break;
            case KVLIST_VALUE:
                stringValue = value.getKvlistValue()
                        .getValuesList()
                        .stream()
                        .map(kv -> kv.getKey() + ": " + anyvalToString(kv.getValue()))
                        .collect(Collectors.joining(","));
                break;
            case BYTES_VALUE:
                stringValue = Arrays.toString(value.getBytesValue().toByteArray());
                break;
            case VALUE_NOT_SET:
                stringValue = "";
                break;
            default:
                stringValue = "[unknown_type]";
                break;
        }
        return stringValue;
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

                final String value = anyvalToString(kv.getValue());
                if (!Strings.isNullOrEmpty(value) && tags.put(key, value) != null) {
                    throw new IllegalStateException("Duplicate key");
                }
            }
            for (Map.Entry<String, String> entry : resourceTags.entrySet()) {
                final String key = entry.getKey();
                final String value = entry.getValue();

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

    private final Function<Integer, IndexToValue> _indexToValueFactory;

    private static final Logger LOGGER = LoggerFactory.getLogger(OpenTelemetryMetricsService.class);
    private static final Logger RATE_LOGGER = LoggerFactory.getRateLimitLogger(OpenTelemetryMetricsService.class, Duration.ofSeconds(30));
    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final Map<String, Unit> UNITS_MAP = Maps.newHashMap();
    static {
        UNITS_MAP.put("ms", Unit.MILLISECOND);
        UNITS_MAP.put("s", Unit.SECOND);
        UNITS_MAP.put("us", Unit.MICROSECOND);
        UNITS_MAP.put("ns", Unit.NANOSECOND);
        UNITS_MAP.put("seconds", Unit.SECOND);
        UNITS_MAP.put("second", Unit.SECOND);
        UNITS_MAP.put("millisecond", Unit.MILLISECOND);
        UNITS_MAP.put("milliseconds", Unit.MILLISECOND);
        UNITS_MAP.put("microsecond", Unit.MICROSECOND);
        UNITS_MAP.put("microseconds", Unit.MICROSECOND);
        UNITS_MAP.put("nanosecond", Unit.NANOSECOND);
        UNITS_MAP.put("nanoseconds", Unit.NANOSECOND);

    }
}
