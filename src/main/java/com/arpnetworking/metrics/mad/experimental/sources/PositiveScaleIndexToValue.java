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
 * Maps otel bucket indexes to values for positive scales.
 *
 * @author Bruno Green
 */
final class PositiveScaleIndexToValue implements IndexToValue {
    private final double _scaleFactor;
    private final double _multiplier;

    PositiveScaleIndexToValue(final int scale) {
        if (scale <= 0) {
            throw new IllegalArgumentException("scale must be positive");
        }

        _scaleFactor = Math.scalb(LOG_BASE2_E, scale);
        _multiplier = 0.5 * (1 + Math.exp(1 / _scaleFactor));
    }

    @Override
    public double map(final int index) {
        return _multiplier * Math.exp(index / _scaleFactor);
    }

    private static final double LOG_BASE2_E = 1D / Math.log(2);
}
