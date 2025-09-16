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
package io.trino.server.ai;

import com.google.inject.Inject;
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.starburst.ai.model.ModelConnectionSpecs;
import io.trino.node.InternalCoordinatorLocator;
import io.trino.server.InternalHttpClient;
import io.trino.spi.connector.ai.ModelConnectionSpecsLoader;

import java.net.URI;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.util.Objects.requireNonNull;

public class RemoteModelConnectionSpecsLoader
        implements ModelConnectionSpecsLoader
{
    public static final String BASE_PATH = "/api/v1/ai/internal/model-connection-specs";
    private static final JsonCodec<ModelConnectionSpecs> CONNECTION_SPECS_CODEC = jsonCodec(ModelConnectionSpecs.class);

    private final HttpClient httpClient;
    private final InternalCoordinatorLocator coordinatorLocator;

    @Inject
    public RemoteModelConnectionSpecsLoader(@InternalHttpClient HttpClient httpClient, InternalCoordinatorLocator coordinatorLocator)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.coordinatorLocator = requireNonNull(coordinatorLocator, "coordinatorLocator is null");
    }

    @Override
    public ModelConnectionSpecs load()
    {
        Request request = prepareGet()
                .setUri(coordinatorUri().resolve(BASE_PATH))
                .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .build();
        FullJsonResponseHandler.JsonResponse<ModelConnectionSpecs> response = httpClient.execute(request, createFullJsonResponseHandler(CONNECTION_SPECS_CODEC));
        return response.getValue();
    }

    private URI coordinatorUri()
    {
        return coordinatorLocator.getCoordinatorUris().stream()
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No active coordinator found"));
    }
}
