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
package io.opentelemetry.sdk.metrics.internal.aggregator;

/**
 * Wraps an internal Otel histogram indexer to allow for use in testing.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public class HistogramIndexer {
    private final Base2ExponentialHistogramIndexer _indexer;

    /**
     * Public constructor.
     *
     * @param scale scale of the histogram buckets
     */
    public HistogramIndexer(final int scale) {
        _indexer = Base2ExponentialHistogramIndexer.get(scale);
    }

    /**
     * Gets the index for a value.
     *
     * @param value the value
     * @return the index
     */
    public int getIndex(final double value) {
        return _indexer.computeIndex(value);
    }
}
