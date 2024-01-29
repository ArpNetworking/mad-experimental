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

import io.grpc.util.AdvancedTlsX509TrustManager;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.SSLContext;

/**
 * Integation test for basic processing of OTLP GRPC requests.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public class OpenTelemetryGrpcIT {
    @Before
    public void reset() {
        GlobalOpenTelemetry.resetForTest();
    }

    @SuppressWarnings("try")
    @Test
    public void test() throws NoSuchAlgorithmException, CertificateException {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
        Logger.getLogger("").setLevel(Level.FINEST);

        final CompletableResultCode result;
        try (OtlpGrpcMetricExporter exporter = OtlpGrpcMetricExporter.builder()
                .setAggregationTemporalitySelector(it -> AggregationTemporality.DELTA)
                .setEndpoint("https://" + System.getProperty("dockerHostAddress") + ":7091")
                .setSslContext(
                        SSLContext.getDefault(),
                        AdvancedTlsX509TrustManager.newBuilder()
                                .setVerification(AdvancedTlsX509TrustManager.Verification.INSECURELY_SKIP_ALL_VERIFICATION)
                                .setSslSocketAndEnginePeerVerifier(null)
                                .build())
                .build()) {
            final InMemoryMetricReader metricReader = InMemoryMetricReader.createDelta();
            final SdkMeterProvider mp = SdkMeterProvider.builder().registerMetricReader(metricReader).build();
            try (OpenTelemetrySdk ignored = OpenTelemetrySdk.builder().setMeterProvider(mp).buildAndRegisterGlobal()) {
                final Meter meter = mp.meterBuilder("mad-experimental").setSchemaUrl("mad").build();
                final DoubleHistogram histo = meter.histogramBuilder("my_histogram").build();

                final Attributes attrs = Attributes.of(
                        AttributeKey.stringKey("service"),
                        "t_service",
                        AttributeKey.stringKey("host"),
                        "l_host",
                        AttributeKey.stringKey("cluster"),
                        "t_cluster");
                histo.record(1.0, attrs);
                histo.record(2.0, attrs);
                histo.record(3.0, attrs);
                histo.record(58.0, attrs);
                result = exporter.export(metricReader.collectAllMetrics()).join(10, TimeUnit.SECONDS);
            }
        }
        Assert.assertTrue(result.isSuccess());
    }
}
