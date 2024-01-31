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

import com.arpnetworking.metrics.mad.model.Record;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.exporter.internal.otlp.metrics.MetricsRequestMarshaler;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.MetricsService;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.View;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import org.apache.pekko.util.ByteString;
import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Performance tests for {@link OpenTelemetryGrpcRecordParser}.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
@Threads(1)
@Fork(1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 3)
@Measurement(iterations = 3, time = 10)
@BenchmarkMode(Mode.Throughput)
public class OtelGrpcRecordParserPT {
    /**
     * Run the benchmarks.
     */
    @Test
    public void runBenchmarks() throws RunnerException {
        final Options options = new OptionsBuilder()
                .include(OtelGrpcRecordParserPT.class.getName())
                .build();
        new Runner(options).run();
    }

    /**
     * Benchmark parsing a record.
     *
     * @param state The benchmark state.
     * @return The parsed records.
     */
    @Benchmark
    public List<Record> testParse(final ParseState state) {
        return state._parser.parse(state._request);
    }


    /**
     * Benchmark state.
     */
    @State(Scope.Thread)
    public static class ParseState {
        /**
         * The request to parse.
         */
        private ExportMetricsServiceRequest _request = null;
        /**
         * The parser to use.
         */
        private OpenTelemetryGrpcRecordParser _parser = null;
        /**
         * Whether to use the cache.
         */
        @Param({"true", "false"})
        private boolean _useCache;

        /**
         * Set up the benchmark state.
         *
         * @throws IOException if the request cannot be created
         */
        @Setup(Level.Trial)
        public void setUp() throws IOException {
            final InMemoryMetricReader metricReader = InMemoryMetricReader.createDelta();;
            final SdkMeterProvider mp =
                    SdkMeterProvider.builder().registerView(
                                    InstrumentSelector.builder()
                                            .setName("my_histogram")
                                            .build(),
                                    View.builder()
                                            .setAggregation(
                                                    Aggregation.base2ExponentialBucketHistogram())
                                            .build())
                            .registerMetricReader(metricReader).build();
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

            _parser = new OpenTelemetryGrpcRecordParser(_useCache);
            _request = createRequest(metricReader);

        }

        private ExportMetricsServiceRequest createRequest(final InMemoryMetricReader reader) throws IOException {
            final MetricsRequestMarshaler marshaller = MetricsRequestMarshaler.create(reader.collectAllMetrics());
            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            marshaller.writeBinaryTo(stream);
            return MetricsService.Serializers.ExportMetricsServiceRequestSerializer.deserialize(ByteString.fromArray(stream.toByteArray()));
        }

    }
}
