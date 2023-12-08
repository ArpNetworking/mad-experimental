/**
 * Copyright 2023 InscopeMetrics.com
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

import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.commons.builder.ThreadLocalBuilder;
import com.arpnetworking.commons.observer.Observable;
import com.arpnetworking.commons.observer.Observer;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.metrics.common.sources.BaseSource;
import com.arpnetworking.metrics.common.sources.Source;
import com.arpnetworking.metrics.mad.model.DefaultMetric;
import com.arpnetworking.metrics.mad.model.DefaultRecord;
import com.arpnetworking.metrics.mad.model.Metric;
import com.arpnetworking.metrics.mad.model.MetricType;
import com.arpnetworking.metrics.mad.model.Quantity;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.metrics.mad.model.statistics.Statistic;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.CalculatedValue;
import com.arpnetworking.tsdcore.model.DefaultKey;
import com.arpnetworking.tsdcore.model.Key;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import net.sf.oval.constraint.NotEmpty;
import net.sf.oval.constraint.NotNull;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Implementation of {@link Source} which wraps another {@link Source}
 * and merges {@link Metric} instances within each {@link Record}
 * together while dropping dimensions.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public final class TagDroppingSource extends BaseSource {

    @Override
    public void start() {
        _source.start();
    }

    @Override
    public void stop() {
        _source.stop();
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("source", _source)
                .put("dropSets", _dropSets)
                .build();
    }

    @Override
    public String toString() {
        return toLogValue().toString();
    }

    private TagDroppingSource(final Builder builder) {
        super(builder);
        _source = builder._source;
        _dropSets = builder._dropSets;

        _source.attach(new DroppingObserver(this, _dropSets));
    }

    private final Source _source;
    private final ImmutableList<DropSet> _dropSets;

    private static final Logger LOGGER = LoggerFactory.getLogger(TagDroppingSource.class);

    // NOTE: Package private for testing
    /* package private */ static final class DroppingObserver implements Observer {

        /* package private */ DroppingObserver(
                final TagDroppingSource source,
                final ImmutableList<DropSet> dropSets) {
            _source = source;
            _dropSets = dropSets;
        }

        @Override
        public void notify(final Observable observable, final Object event) {
            if (!(event instanceof Record)) {
                LOGGER.error()
                        .setMessage("Observed unsupported event")
                        .addData("event", event)
                        .log();
                return;
            }

            // Merge the metrics in the record together
            final Record record = (Record) event;
            final Map<Key, Map<String, MergingMetric>> mergedMetrics = Maps.newHashMap();

            for (final Map.Entry<String, ? extends Metric> metric : record.getMetrics().entrySet()) {
                final String metricName = metric.getKey();
                final List<String> dimensionsToDrop = _dropSets.stream()
                        .filter(d -> d._metricPattern.matcher(metricName).matches())
                        .flatMap(d -> d._removeDimensions.stream())
                        .collect(Collectors.toList());
                    merge(
                            metricName,
                            metric.getValue(),
                            mergedMetrics,
                            getModifiedDimensions(record.getDimensions(), dimensionsToDrop));
            }

            // Raise the merged record event with this source's observers
            // NOTE: Do not leak instances of MergingMetric since it is mutable
            for (final Map.Entry<Key, Map<String, MergingMetric>> entry : mergedMetrics.entrySet()) {
                _source.notify(
                        ThreadLocalBuilder.build(
                                DefaultRecord.Builder.class,
                                b1 -> b1.setMetrics(
                                                entry.getValue().entrySet().stream().collect(
                                                        ImmutableMap.toImmutableMap(
                                                                Map.Entry::getKey,
                                                                e -> ThreadLocalBuilder.clone(
                                                                        e.getValue(),
                                                                        DefaultMetric.Builder.class))))
                                        .setId(record.getId())
                                        .setTime(record.getTime())
                                        .setAnnotations(record.getAnnotations())
                                        .setDimensions(entry.getKey().getParameters())));
            }
        }

        private Key getModifiedDimensions(
                final ImmutableMap<String, String> inputDimensions,
                final List<String> remove) {
            final Map<String, String> finalTags = Maps.newHashMap(inputDimensions);
            remove.forEach(finalTags::remove);

            return new DefaultKey(ImmutableMap.copyOf(finalTags));
        }

        private void merge(
                final String metricName,
                final Metric metric,
                final Map<Key, Map<String, MergingMetric>> mergedMetrics,
                final Key dimensionKey) {

            final Map<String, MergingMetric> mergedMetricsForDimensions =
                    mergedMetrics.computeIfAbsent(dimensionKey, k -> Maps.newHashMap());
            final MergingMetric mergedMetric = mergedMetricsForDimensions.get(metricName);
            if (mergedMetric == null) {
                // This is the first time this metric is being merged into
                mergedMetricsForDimensions.put(metricName, new MergingMetric(metric));
            } else if (!mergedMetric.isMergable(metric)) {
                // This instance of the metric is not mergable with previous
                LOGGER.error()
                        .setMessage("Discarding metric")
                        .addData("reason", "failed to merge")
                        .addData("metric", metric)
                        .addData("mergedMetric", mergedMetric)
                        .log();
            } else {
                // Merge the new instance in
                mergedMetric.merge(metric);
            }
        }

        private final TagDroppingSource _source;
        private final ImmutableList<DropSet> _dropSets;
    }

    // NOTE: Package private for testing
    /* package private */ static final class MergingMetric implements Metric {


        /* package private */ MergingMetric(final Metric metric) {
            _type = metric.getType();
            _values.addAll(metric.getValues());
            _statistics = metric.getStatistics();
        }

        public boolean isMergable(final Metric metric) {
            return _type.equals(metric.getType());
        }

        public void merge(final Metric metric) {
            if (!isMergable(metric)) {
                throw new IllegalArgumentException(String.format("Metric cannot be merged; metric=%s", metric));
            }
            _values.addAll(metric.getValues());
        }

        @Override
        public MetricType getType() {
            return _type;
        }

        @Override
        public ImmutableList<Quantity> getValues() {
            return _values.build();
        }

        @Override
        public ImmutableMap<Statistic, ImmutableList<CalculatedValue<?>>> getStatistics() {
            return _statistics;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("id", Integer.toHexString(System.identityHashCode(this)))
                    .add("Type", _type)
                    .add("Values", _values)
                    .toString();
        }

        private final MetricType _type;
        private final ImmutableList.Builder<Quantity> _values = ImmutableList.builder();
        private final ImmutableMap<Statistic, ImmutableList<CalculatedValue<?>>> _statistics;
    }

    /**
     * Represents a set of transformations to apply.
     *
     * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
     */
    public static final class DropSet {
        public Pattern getMetricPattern() {
            return _metricPattern;
        }

        public ImmutableList<String> getRemoveDimensions() {
            return _removeDimensions;
        }

        private DropSet(final Builder builder) {
            _metricPattern = builder._metricPattern;
            _removeDimensions = builder._removeDimensions;
        }

        private final Pattern _metricPattern;
        private final ImmutableList<String> _removeDimensions;

        /**
         * Implementation of the builder pattern for a {@link DropSet}.
         */
        public static final class Builder extends OvalBuilder<DropSet> {
            /**
             * Public constructor.
             */
            public Builder() {
                super(DropSet::new);
            }

            /**
             * Sets dimensions to remove. Optional. Cannot be null. Defaults to empty.
             *
             * @param value List of dimensions to inject.
             * @return This instance of {@code Builder}.
             */
            public Builder setRemoveDimensions(final ImmutableList<String> value) {
                _removeDimensions = value;
                return this;
            }

            /**
             * Sets the metric pattern. Required. Cannot be null. Cannot be empty.
             *
             * @param value The regex to match metrics against.
             * @return This instance of {@link Builder}.
             */
            public Builder setMetricPattern(final Pattern value) {
                _metricPattern = value;
                return this;
            }

            @NotNull
            @NotEmpty
            private Pattern _metricPattern;
            @NotNull
            private ImmutableList<String> _removeDimensions = ImmutableList.of();
        }
    }

    /**
     * Implementation of builder pattern for {@link TagDroppingSource}.
     *
     * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
     */
    public static final class Builder extends BaseSource.Builder<Builder, TagDroppingSource> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(TagDroppingSource::new);
        }

        /**
         * Sets the underlying source. Cannot be null.
         *
         * @param value The underlying source.
         * @return This instance of {@link Builder}.
         */
        public Builder setSource(final Source value) {
            _source = value;
            return this;
        }

        /**
         * Sets the transformations. Required. Cannot be null. Cannot be empty.
         *
         * @param value The list of transformations to apply.
         * @return This instance of {@link Builder}.
         */
        public Builder setDropSets(final ImmutableList<DropSet> value) {
            _dropSets = value;
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @NotNull
        private Source _source;
        @NotNull
        @NotEmpty
        private ImmutableList<DropSet> _dropSets = ImmutableList.of();
    }
}
