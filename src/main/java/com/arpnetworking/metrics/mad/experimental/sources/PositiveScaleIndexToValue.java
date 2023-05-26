package com.arpnetworking.metrics.mad.experimental.sources;

final class PositiveScaleIndexToValue implements IndexToValue {
    private final int _scale;
    private final double _scaleFactor;
    private final double _multiplier;

    public PositiveScaleIndexToValue(final int scale) {
        if(scale <= 0) {
            throw new RuntimeException("scale must be positive");
        }
        _scale = scale;
        _scaleFactor = Math.scalb(LOG_BASE2_E, scale);
        _multiplier = 0.5 * (1 + Math.exp(1/_scaleFactor));
    }

    @Override
    public double map(final int index) {
        final double value;
        value = _multiplier * Math.exp(index / _scaleFactor);
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
