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

import com.arpnetworking.commons.jackson.databind.ObjectMapperFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;

/**
 * Integation test for health check (e.g. /ping).
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot io)
 */
public class HealthCheckIT {

    @Test
    public void test() throws IOException {
        final JsonNode expectedResponse = OBJECT_MAPPER.readTree("{\"status\":\"HEALTHY\"}");
        final JsonNode actualResponse = OBJECT_MAPPER.readTree(
                new URL(getEndpoint() + "/ping"));
        Assert.assertEquals(expectedResponse, actualResponse);
    }

    private static String getEndpoint() {
        return "http://" + System.getProperty("dockerHostAddress") + ":7090";
    }

    private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();
}
