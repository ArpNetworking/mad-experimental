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

import io.opentelemetry.sdk.metrics.internal.aggregator.HistogramIndexer;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Performance tests for the {@link OpenTelemetryGrpcRecordParser} class.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
@State(Scope.Benchmark)
@Threads(1)
@Fork(1)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.All)
public class IndexToValuePT {


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

}
