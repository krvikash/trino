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
package io.starburst.materialization.metastore.client;

import com.google.inject.Inject;
import dev.failsafe.RetryPolicy;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler.StringResponse;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.starburst.materialization.metastore.MetastoreId;
import io.starburst.materialization.metastore.RawMaterializationDefinition;
import io.starburst.materialization.metastore.RawMaterializationMetastore;
import io.starburst.materialization.metastore.StorageTableId;
import io.trino.spi.connector.CatalogSchemaTableName;

import javax.net.ssl.SSLHandshakeException;

import java.net.URI;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Throwables.getCausalChain;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HeaderNames.CONTENT_TYPE;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.Request.Builder.preparePut;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.starburst.materialization.metastore.client.RetryingHttpClient.isRetryableException;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static jakarta.ws.rs.core.Response.Status.OK;
import static java.util.Objects.requireNonNull;

public class HttpMaterializationMetastore
        implements RawMaterializationMetastore
{
    private static final Logger log = Logger.get(HttpMaterializationMetastore.class);

    private static final JsonCodec<RawMaterializationDefinition> DEFINITION_CODEC = jsonCodec(RawMaterializationDefinition.class);
    private static final JsonCodec<List<RawMaterializationDefinition>> DEFINITION_LIST_CODEC = listJsonCodec(RawMaterializationDefinition.class);
    private static final JsonCodec<RenameRequest> RENAME_CODEC = jsonCodec(RenameRequest.class);

    private static final RetryPolicy<Object> METASTORE_READ_RETRY_POLICY = RetryPolicy.builder()
            .withMaxRetries(5)
            .withBackoff(100, 5_000, ChronoUnit.MILLIS, 2.0)
            .handleIf(HttpMaterializationMetastore::isMetastoreReadRetryable)
            .onRetry(e -> log.warn(e.getLastException(), "Retrying metastore read request (attempt %s)", e.getAttemptCount()))
            .onRetriesExceeded(e -> log.error(e.getException(), "Metastore read request failed, all retries exhausted"))
            .build();

    private final URI baseUri;
    private final MetastoreId metastoreId;
    private final HttpClient httpClient;
    private final RequestAuthenticator requestAuthenticator;

    @Inject
    public HttpMaterializationMetastore(
            MaterializationMetastoreClientConfig config,
            @ForMaterializationMetastoreClient HttpClient httpClient,
            RequestAuthenticator requestAuthenticator)
    {
        this.baseUri = requireNonNull(config.getBaseUri(), "baseUri is null");
        this.metastoreId = new MetastoreId(requireNonNull(config.getMetastoreId(), "metastoreId is null"));
        this.httpClient = new RetryingHttpClient(httpClient, METASTORE_READ_RETRY_POLICY)
                .withHttpStatusListener(new InvalidServiceStatusListener());
        this.requestAuthenticator = requireNonNull(requestAuthenticator, "requestAuthenticator is null");
    }

    @Override
    public List<RawMaterializationDefinition> listMaterializations()
    {
        Request request = authenticated(prepareGet()
                .setUri(materializationsUri().build()))
                .build();
        JsonResponse<List<RawMaterializationDefinition>> response = httpClient.execute(request, createFullJsonResponseHandler(DEFINITION_LIST_CODEC));
        checkOk(response.getStatusCode(), response.getResponseBody());
        if (!response.hasValue()) {
            throw new RuntimeException("Response does not contain a JSON value", response.getException());
        }
        return response.getValue();
    }

    @Override
    public void createOrReplace(RawMaterializationDefinition definition)
    {
        Request request = authenticated(preparePut()
                .setUri(materializationsUri().build())
                .addHeader(CONTENT_TYPE, APPLICATION_JSON)
                .setBodyGenerator(jsonBodyGenerator(DEFINITION_CODEC, definition)))
                .build();
        execute(request);
    }

    @Override
    public void remove(CatalogSchemaTableName materializedViewName)
    {
        Request request = authenticated(prepareDelete()
                .setUri(materializationsUri()
                        .appendPath(materializedViewName.getCatalogName())
                        .appendPath(materializedViewName.getSchemaTableName().getSchemaName())
                        .appendPath(materializedViewName.getSchemaTableName().getTableName())
                        .build()))
                .build();
        execute(request);
    }

    @Override
    public void renameIfExists(CatalogSchemaTableName source, CatalogSchemaTableName target, StorageTableId targetStorageTableId)
    {
        Request request = authenticated(preparePost()
                .setUri(materializationsUri().appendPath("rename").build())
                .addHeader(CONTENT_TYPE, APPLICATION_JSON)
                .setBodyGenerator(jsonBodyGenerator(RENAME_CODEC, new RenameRequest(source, target, targetStorageTableId))))
                .build();
        execute(request);
    }

    private Request.Builder authenticated(Request.Builder builder)
    {
        requestAuthenticator.accept(builder);
        return builder;
    }

    private HttpUriBuilder materializationsUri()
    {
        return uriBuilderFrom(baseUri)
                .replacePath("/v1")
                .appendPath(metastoreId.id())
                .appendPath("materializations");
    }

    private void execute(Request request)
    {
        StringResponse response = httpClient.execute(request, createStringResponseHandler());
        checkOk(response.getStatusCode(), response.getBody());
    }

    private static void checkOk(int statusCode, String body)
    {
        if (statusCode != OK.getStatusCode()) {
            throw new RuntimeException("Unexpected response: " + statusCode + "; " + body);
        }
    }

    private static boolean isMetastoreReadRetryable(Throwable t)
    {
        return isRetryableException(t) ||
                getCausalChain(t).stream().anyMatch(e ->
                        (e instanceof TimeoutException) ||
                                (e instanceof SSLHandshakeException));
    }
}
