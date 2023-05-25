package io.opentelemetry.sdk.metrics.internal.aggregator;

public class HistogramIndexer {
    private final Base2ExponentialHistogramIndexer _indexer;

    public HistogramIndexer(int scale) {
        _indexer = Base2ExponentialHistogramIndexer.get(scale);
    }

    public int getIndex(double value) {
        return _indexer.computeIndex(value);
    }
}
