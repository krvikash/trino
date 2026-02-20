/*
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
package com.starburstdata.plugin.openapi;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.trino.spi.connector.ConnectorTableHandle;

import java.net.URI;

import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static com.google.common.net.MediaType.JSON_UTF_8;

public record OpenApiRequestTableHandle(
        @JsonProperty("path") String path)
        implements ConnectorTableHandle
{
    private static final String USER_AGENT_VALUE = "starburst-openapi";
    private static final String JSON_MEDIA_TYPE = JSON_UTF_8.toString();

    @JsonIgnore
    public Request toInitialRequest(URI baseUri)
    {
        return Request.builder()
                .setMethod("GET")
                .setUri(HttpUriBuilder.uriBuilderFrom(baseUri)
                        .appendPath(path)
                        .build())
                .addHeader(USER_AGENT, USER_AGENT_VALUE)
                .addHeader(CONTENT_TYPE, JSON_MEDIA_TYPE)
                .addHeader(ACCEPT, JSON_MEDIA_TYPE)
                .build();
    }
}
