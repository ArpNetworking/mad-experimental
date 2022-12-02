/*
 * Copyright 2022 Inscope Metrics, Inc.
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
package com.arpnetworking.metrics.mad.experimental.sinks;

import com.arpnetworking.commons.jackson.databind.ObjectMapperFactory;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.metrics.mad.model.AggregatedData;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.tsdcore.sinks.BaseSink;
import com.arpnetworking.tsdcore.sinks.Sink;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link Sink} which records time series.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
/* package private */ final class MetricSeriesLoggingSink extends BaseSink {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricSeriesLoggingSink.class);
    private final Map<String, List<Map<String, String>>> _metrics = Maps.newConcurrentMap();
    private final ObjectMapper _mapper;
    private ZonedDateTime _currentTime = ZonedDateTime.now();


    private MetricSeriesLoggingSink(final Builder builder) {
        super(builder);
        _mapper = ObjectMapperFactory.getInstance();
    }

    @LogValue
    @Override
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("super", super.toLogValue())
                .put("mapper", _mapper)
                .build();
    }

    @Override
    public void recordAggregateData(final PeriodicData periodicData) {
        recordMetrics(periodicData);

        final ImmutableMap<String, String> dimensions = periodicData.getDimensions().getParameters();
        for (Map.Entry<String, Collection<AggregatedData>> entry : periodicData.getData().asMap().entrySet()) {
            _metrics.compute(entry.getKey(), (k, v) -> {
                List<Map<String, String>> list = v;
                if (list == null) {
                    list = Lists.newArrayList();
                }
                list.add(dimensions);
                return list;
            });
        }
    }

    private synchronized void recordMetrics(final PeriodicData periodicData) {
        final ZonedDateTime time = periodicData.getStart();
        if (time.isAfter(_currentTime)) {
            _currentTime = time;
            LOGGER.info().setMessage("Dumping metrics streams").log();
            try (OutputStream outputStream = Files.newOutputStream(
                    Path.of("metrics_streams.json"),
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.CREATE)) {
                _mapper.writerWithDefaultPrettyPrinter().writeValue(outputStream, _metrics);
            } catch (final IOException e) {
                LOGGER.error().setMessage("Unable to serialize time series").setThrowable(e).log();
            }
            _metrics.clear();
            LOGGER.info().setMessage("Dumping metrics streams complete").log();
        }
    }

    @Override
    public void close() {
        _metrics.clear();
    }


    /**
     * Builder for {@link MetricSeriesLoggingSink}.
     *
     * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
     */
    public static final class Builder extends BaseSink.Builder<Builder, MetricSeriesLoggingSink> {
        Builder() {
            super(MetricSeriesLoggingSink::new);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
