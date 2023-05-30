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
 * Maps otel bucket indexes to values using base 2 arithmetic.
 *
 * @author Bruno Green
 */
final class Base2IndexToValue implements IndexToValue {
    private final double _base;
    private final double _multiplier;

    Base2IndexToValue(final int scale) {
        _base = Math.pow(2, Math.pow(2, -scale));
        _multiplier = 0.5 * (_base + 1);
    }

    @Override
    public double map(final int index) {
        return _multiplier * Math.pow(_base, index);
    }
}
