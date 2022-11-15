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

import com.arpnetworking.metrics.mad.model.AggregatedData;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.tsdcore.sinks.BaseSink;
import com.arpnetworking.tsdcore.sinks.Sink;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import net.sf.oval.constraint.NotNull;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link Sink} which records time series.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
/* package private */ final class MetricsSeriesLoggingSink extends BaseSink {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsSeriesLoggingSink.class);
    private final Map<String, List<Map<String, String>>> _metrics = Maps.newTreeMap();
    private final ObjectMapper _mapper;
    private ZonedDateTime _currentTime = ZonedDateTime.now();


    private MetricsSeriesLoggingSink(final Builder builder) {
        super(builder);
        _mapper = builder._objectMapper;
    }

    @Override
    public void recordAggregateData(final PeriodicData periodicData) {
        final ZonedDateTime time = periodicData.getStart();
        if (time.isAfter(_currentTime)) {
            try {
                recordMetrics();
            } catch (final JsonProcessingException e) {
                LOGGER.error().setMessage("Unable to serialize time series").setThrowable(e).log();
            }
            _metrics.clear();
        }

        final ImmutableMap<String, String> dimensions = periodicData.getDimensions().getParameters();
        for (Map.Entry<String, AggregatedData> entry : periodicData.getData().entries()) {
            _metrics.compute(entry.getKey(), (k, v) -> {
                if (v == null) {
                    v = Lists.newArrayList();
                }
                v.add(dimensions);
                return v;
            });
        }
    }

    private void recordMetrics() throws JsonProcessingException {
        LOGGER.info().setMessage("Recording time series").addData("series", _mapper.writeValueAsString(_metrics)).log();
    }

    @Override
    public void close() {
        _metrics.clear();
    }


    /**
     * Builder for {@link MetricsSeriesLoggingSink}.
     *
     * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
     */
    public static final class Builder extends BaseSink.Builder<Builder, MetricsSeriesLoggingSink> {
        public Builder() {
            super(MetricsSeriesLoggingSink::new);
        }

        /**
         * Sets the {@link ObjectMapper}.
         * @param value the value
         * @return self
         */
        public Builder setObjectMapper(final ObjectMapper value) {
            _objectMapper = value;
            return self();
        }

        @Override
        protected Builder self() {
            return this;
        }

        @JacksonInject
        @NotNull
        private ObjectMapper _objectMapper;
    }
}
