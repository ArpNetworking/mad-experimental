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
 * Maps otel bucket indexes to values for 0 scale.
 *
 * @author Bruno Green
 */
class ZeroScaleIndexToValue implements IndexToValue {
    @Override
    public double map(final int index) {
        final double value;
        final double low = mapIndexToValueScaleZero(index);
        final double high = mapIndexToValueScaleZero(index + 1);
        value = (low + high) / 2;
        return value;
    }

    protected static double mapIndexToValueScaleZero(final int index) {
        final long rawExponent = index + EXPONENT_BIAS;
        final long rawDouble = rawExponent << MANTISSA_WIDTH;
        return Double.longBitsToDouble(rawDouble);

    }

    private static final long EXPONENT_BIAS = (1 << 10) - 1;
    private static final long MANTISSA_WIDTH = 52;
}
