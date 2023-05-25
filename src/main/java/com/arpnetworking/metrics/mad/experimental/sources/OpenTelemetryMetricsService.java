/*
 * Copyright 2021 Inscope Metrics, Inc.
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

import akka.actor.ActorSystem;
import akka.pattern.Patterns;
import com.arpnetworking.metrics.mad.model.Record;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.MetricsService;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Implementation of the GRPC Metrics service.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public class OpenTelemetryMetricsService implements MetricsService {

    /**
     * Public constructor.
     *
     * @param actorSystem {@link ActorSystem} the source is running in
     */
    public OpenTelemetryMetricsService(final ActorSystem actorSystem) {
        _actorSystem = actorSystem;
        _recordParser = new OpenTelemetryGrpcRecordParser();
    }

    @Override
    public CompletionStage<ExportMetricsServiceResponse> export(final ExportMetricsServiceRequest in) {
        final List<Record> records = _recordParser.parse(in);

        return _actorSystem.actorSelection("/user/" + OpenTelemetryGrpcSource.ACTOR_NAME)
                .resolveOne(Duration.ofSeconds(1))
                .thenApply(a -> Patterns.ask(a, new OpenTelemetryGrpcSource.RecordsMessage(records), Duration.ofSeconds(3)))
                .thenApply(v -> ExportMetricsServiceResponse.newBuilder().build());
    }



    private final ActorSystem _actorSystem;
    private final OpenTelemetryGrpcRecordParser _recordParser;

}
