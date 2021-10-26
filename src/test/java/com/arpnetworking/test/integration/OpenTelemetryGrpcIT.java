/*
 * Copyright 2016 Inscope Metrics, Inc.
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
package com.arpnetworking.test.integration;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.BoundDoubleHistogram;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.common.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.export.MetricReaderFactory;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.metrics.testing.InMemoryMetricReader;
import io.opentelemetry.sdk.metrics.view.Aggregation;
import io.opentelemetry.sdk.metrics.view.InstrumentSelector;
import io.opentelemetry.sdk.metrics.view.View;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Integation test for basic processing of OTLP GRPC requests.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public class OpenTelemetryGrpcIT {

    @Test
    public void test() throws IOException, InterruptedException {
        final ManagedChannel channel = NettyChannelBuilder.forAddress(System.getProperty("dockerHostAddress"), 7091)
                .sslContext(GrpcSslContexts.forClient()
                        .trustManager(InsecureTrustManagerFactory.INSTANCE)
                        .build())
                .build();

        final OtlpGrpcMetricExporter exporter = OtlpGrpcMetricExporter.builder().setChannel(channel).build();
        final InMemoryMetricReader metricReader = new InMemoryMetricReader();
        final MeterProvider mp = SdkMeterProvider.builder().registerMetricReader(metricReader).buildAndRegisterGlobal();

        final Meter meter = mp.meterBuilder("mad-experimental").setSchemaUrl("mad").build();
        final DoubleHistogram histo = meter.histogramBuilder("my_histogram").build();

        final BoundDoubleHistogram bound = histo.bind(Attributes.of(
                AttributeKey.stringKey("service"),
                "t_service",
                AttributeKey.stringKey("host"),
                "l_host",
                AttributeKey.stringKey("cluster"),
                "t_cluster"));
        bound.record(1.0);
        bound.record(2.0);
        bound.record(3.0);
        final CompletableResultCode result = exporter.export(metricReader.collectAllMetrics()).join(10, TimeUnit.SECONDS);
        Assert.assertTrue(result.isSuccess());
    }

    @Test
    @Ignore
    public void produceMetrics() throws IOException, InterruptedException {
        final ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", 7091)
                .sslContext(GrpcSslContexts.forClient()
                        .trustManager(InsecureTrustManagerFactory.INSTANCE)
                        .build())
                .build();

        final OtlpGrpcMetricExporter exporter = OtlpGrpcMetricExporter.builder().setChannel(channel).build();
        final MetricReaderFactory metricReader = PeriodicMetricReader.create(exporter, Duration.ofSeconds(3));
        final MeterProvider mp = SdkMeterProvider.builder()
                .registerMetricReader(metricReader)
                .registerView(
                        InstrumentSelector.builder().setInstrumentType(InstrumentType.HISTOGRAM).build(),
                        View.builder().
                                setAggregation(Aggregation.explicitBucketHistogram(AggregationTemporality.DELTA))
                                .build())
                .buildAndRegisterGlobal();

        final Meter meter = mp.meterBuilder("mad-experimental").setSchemaUrl("mad").build();
        final DoubleHistogram histo = meter.histogramBuilder("my_histogram").build();

        final BoundDoubleHistogram bound = histo.bind(Attributes.of(
                AttributeKey.stringKey("service"),
                "t_service",
                AttributeKey.stringKey("host"),
                "l_host",
                AttributeKey.stringKey("cluster"),
                "t_cluster"));
        for (int x = 0; x < 600; x++) {
            bound.record(1.0);
            bound.record(2.0);
            bound.record(3.0);
            Thread.sleep(500);
        }
    }
}
