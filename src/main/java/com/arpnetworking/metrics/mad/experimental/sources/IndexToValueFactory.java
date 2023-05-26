package com.arpnetworking.metrics.mad.experimental.sources;

class IndexToValueFactory {
    public static IndexToValue create(int scale) {
        if(scale > 0) {
            return new PositiveScaleIndexToValue(scale);
        } else if(scale == 0) {
            return new ZeroScaleIndexToValue();
        } else {
            return new NegativeScaleIndexToValue(scale);
        }
    }
}
