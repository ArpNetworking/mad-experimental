package com.arpnetworking.metrics.mad.experimental.sources;

final class NegativeScaleIndexToValue extends ZeroScaleIndexToValue {
    private final int _scale;

    public NegativeScaleIndexToValue(final int scale) {
        if(scale >= 0) {
            throw new RuntimeException("scale must be negative");
        }
        _scale = scale;
    }
    @Override
    public double map(int index) {
        final double value;
        final double low = mapIndexToValueScaleZero(index << -_scale);
        final double high = mapIndexToValueScaleZero((index + 1) << -_scale);
        value = (low + high) / 2;
        return value;
    }
}
