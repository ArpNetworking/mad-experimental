package com.arpnetworking.metrics.mad.experimental.sources;

final class PositiveScaleIndexToValue implements IndexToValue {
    private final int _scale;
    private final double _scaleFactor;

    public PositiveScaleIndexToValue(final int scale) {
        _scale = scale;
        _scaleFactor = Math.scalb(LOG_BASE2_E, scale);
    }

    @Override
    public double map(final int index) {
        final double value;
        if (_scale > 0) {
            final double low = Math.exp(index / _scaleFactor);
            final double high = Math.exp((index + 1) / _scaleFactor);
            value = (low + high) / 2;
        } else if (_scale == 0) {
            // For scale zero, compute the exact index by extracting the exponent
            final double low = mapIndexToValueScaleZero(index);
            final double high = mapIndexToValueScaleZero(index + 1);
            value = (low + high) / 2;
        } else {
            // For negative scales, compute the exact index by extracting the exponent and shifting it to
            // the right by -scale
            final double low = mapIndexToValueScaleZero(index << -_scale);
            final double high = mapIndexToValueScaleZero((index + 1) << -_scale);
            value = (low + high) / 2;
        }
        return value;
    }

    private static double mapIndexToValueScaleZero(final int index) {
        final long rawExponent = index + EXPONENT_BIAS;
        final long rawDouble = rawExponent << MANTISSA_WIDTH;
        return Double.longBitsToDouble(rawDouble);

    }
    private static final long EXPONENT_BIAS = (1 << 10) - 1;
    private static final long MANTISSA_WIDTH = 52;
    private static final double LOG_BASE2_E = 1D / Math.log(2);
}
