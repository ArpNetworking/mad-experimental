package com.arpnetworking.metrics.mad.experimental.sources;

final class Base2IndexToValue implements IndexToValue {
    final double _base;
    final double _multiplier;
    public Base2IndexToValue(int scale) {
        _base = Math.pow(2, Math.pow(2, -scale));
        _multiplier = 0.5*(_base + 1);
    }

    @Override
    public double map(int index) {
        return _multiplier * Math.pow(_base, index);
    }
}
