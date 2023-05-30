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

import org.junit.Ignore;
import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * Performance tests for a variety of IndexToValue classes.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
@State(Scope.Benchmark)
@Threads(1)
@Fork(1)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 1, time = 5)
@BenchmarkMode(Mode.Throughput)
public class IndexToValuePT {

    private final int _index = 15;
    private final Base2IndexToValue _base2IndexToValue = new Base2IndexToValue(1);
    private final PositiveScaleIndexToValue _positiveScaleIndexToValue = new PositiveScaleIndexToValue(1);
    private final NegativeScaleIndexToValue _negativeScaleIndexToValue = new NegativeScaleIndexToValue(-1);
    private final ZeroScaleIndexToValue _zeroScaleIndexToValue = new ZeroScaleIndexToValue();


    /**
     * Run the benchmarks.
     */
    @Test
    @Ignore("Used only to benchmark the scales. Base2 is far slower per results. Run manually to re-benchmark.")
    public void runBenchmarks() throws RunnerException {
        final Options options = new OptionsBuilder()
                .include(IndexToValuePT.class.getName())
                .build();
        new Runner(options).run();
    }


    /**
     * base 2 indexer benchmark.
     *
     * @return the value
     */
    @Benchmark
    public double testBase2Indexer() {
       return _base2IndexToValue.map(_index);
    }

    /**
     * Positive scale indexer benchmark.
     *
     * @return the value
     */
    @Benchmark
    public double testPositiveScaleIndexer() {
       return _positiveScaleIndexToValue.map(_index);
    }

    /**
     * Negative scale indexer benchmark.
     *
     * @return the value
     */
    @Benchmark
    public double testNegativeScaleIndexer() {
       return _negativeScaleIndexToValue.map(_index);
    }

    /**
     * Zero scale indexer benchmark.
     *
     * @return the value
     */
    @Benchmark
    public double testZeroScaleIndexer() {
        return _zeroScaleIndexToValue.map(_index);
    }
}
