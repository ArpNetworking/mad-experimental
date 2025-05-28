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

/**
 * Maps otel bucket indexes to values for negative scales.
 *
 * @author Bruno Green
 */
final class NegativeScaleIndexToValue extends ZeroScaleIndexToValue {
    private final int _scale;

    /**
     * Constructor.
     *
     * @param scale the scale of the buckets
     */
     NegativeScaleIndexToValue(final int scale) {
        if (scale >= 0) {
            throw new IllegalArgumentException("scale must be negative");
        }
        _scale = scale;
    }
    @Override
    public double map(final int index) {
        final double value;
        final double low = mapIndexToValueScaleZero(index << -_scale);
        final double high = mapIndexToValueScaleZero((index + 1) << -_scale);
        value = (low + high) / 2;
        return value;
    }
}
