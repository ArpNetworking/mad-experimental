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

import akka.actor.AbstractActor;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.japi.function.Function;
import com.arpnetworking.http.SupplementalRoutes;
import com.arpnetworking.metrics.common.sources.ActorSource;
import com.arpnetworking.metrics.common.sources.BaseSource;
import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.arpnetworking.metrics.mad.model.Record;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceHandlerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import javax.inject.Inject;

/**
 * Source that uses OpenTelemetry GRPC as input.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
 */
public final class OpenTelemetryGrpcSource extends ActorSource {
    @Override
    protected Props createProps() {
        return Actor.props(this);
    }

    @Override
    protected void notify(final Object event) {
        super.notify(event);
    }

    private OpenTelemetryGrpcSource(final Builder builder) {
        super(builder);
    }

    /**
     * Name of the actor created to receive the requests.
     */
    public static final String ACTOR_NAME = "opentelgrpc";

    /**
     * Public message class to inform the actor of records.
     */
    public static final class RecordsMessage {
        private final List<Record> _records;

        /**
         * Public constructor.
         * @param records records to pass to the source actor
         */
        public RecordsMessage(final List<Record> records) {
            _records = records;
        }

        public List<Record> getRecords() {
            return _records;
        }
    }

    /**
     * Internal actor to process requests.
     */
    /* package private */ static final class Actor extends AbstractActor {
        /**
         * Creates a {@link Props} for this actor.
         *
         * @param source The {@link OpenTelemetryGrpcSource} to send notifications through.
         * @return A new {@link Props}
         */
        /* package private */ static Props props(final OpenTelemetryGrpcSource source) {
            return Props.create(Actor.class, source);
        }


        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(RecordsMessage.class, this::processRecordMessage)
                    .build();
        }

        private void processRecordMessage(final RecordsMessage message) {
            LOGGER.trace()
                    .setMessage("Got records")
                    .addData("records", message.getRecords())
                    .addData("count", message.getRecords().size()).log();
            message.getRecords().forEach(_sink::notify);
            sender().tell("OK", self());
        }

        /**
         * Constructor.
         *
         * @param source The {@link OpenTelemetryGrpcSource} to send notifications through.
         */
        /* package private */ Actor(final OpenTelemetryGrpcSource source) {
            _sink = source;
        }

        private final OpenTelemetryGrpcSource _sink;
        private static final Logger LOGGER = LoggerFactory.getLogger(Actor.class);
    }

    /**
     * OpenTelemetryGrpcSource {@link BaseSource.Builder} implementation.
     *
     * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
     */
    public static final class Builder extends ActorSource.Builder<Builder, OpenTelemetryGrpcSource> {
        /**
         * Public constructor.
         */
        public Builder() {
            super(OpenTelemetryGrpcSource::new);
            setActorName(ACTOR_NAME);
        }

        @Override
        protected OpenTelemetryGrpcSource.Builder self() {
            return this;
        }
    }

    /**
     * Routes for handling the the OpenTelemetry GRPC service.
     *
     * @author Brandon Arp (brandon dot arp at inscopemetrics dot io)
     */
    public static class Routes implements SupplementalRoutes {

        /**
         * Public constructor.
         * @param actorSystem ActorSystem to find the source actor in
         * @param periodicMetrics A PeriodicMetrics to record metrics against
         */
        @Inject
        public Routes(final ActorSystem actorSystem, final PeriodicMetrics periodicMetrics) {
            _actorSystem = actorSystem;
            _metrics = periodicMetrics;
        }

        @Override
        public Optional<CompletionStage<HttpResponse>> apply(final HttpRequest param) throws Exception {
            final Function<HttpRequest, CompletionStage<HttpResponse>> subrouter =
                    MetricsServiceHandlerFactory.create(new OpenTelemetryMetricsService(_actorSystem), _actorSystem);
            return Optional.of(subrouter.apply(param));
        }

        private static final long serialVersionUID = 1L;
        @SuppressFBWarnings("SE_BAD_FIELD")
        private final ActorSystem _actorSystem;
        @SuppressFBWarnings("SE_BAD_FIELD")
        private final PeriodicMetrics _metrics;
    }
}
