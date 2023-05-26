package com.arpnetworking.metrics.mad.experimental.sources;

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
