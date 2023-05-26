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
 * Factory class for creating IndexToValue instances.
 *
 * @author Bruno Green
 */
final class IndexToValueFactory {
    private IndexToValueFactory() {}

    /**
     * Create an IndexToValue instance.
     *
     * @param scale the scale of the buckets
     * @return a new IndexToValue instance
     */
    public static IndexToValue create(final int scale) {
        if (scale > 0) {
            return new PositiveScaleIndexToValue(scale);
        } else if (scale == 0) {
            return new ZeroScaleIndexToValue();
        } else {
            return new NegativeScaleIndexToValue(scale);
        }
    }
}
