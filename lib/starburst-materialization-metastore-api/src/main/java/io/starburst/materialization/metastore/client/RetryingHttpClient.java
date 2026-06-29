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

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatusListener;
import io.airlift.http.client.Request;
import io.airlift.http.client.RequestStats;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.StreamingResponse;
import io.airlift.log.Logger;
import jakarta.annotation.PreDestroy;

import java.io.EOFException;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Throwables.getCausalChain;
import static java.util.Objects.requireNonNull;

// Copied from io.starburst.stargate.http.RetryingHttpClient.
// TODO: Replace it with the common class once available in cork
public class RetryingHttpClient
        implements HttpClient
{
    private static final Logger log = Logger.get(RetryingHttpClient.class);
    private static final RetryPolicy<Object> DEFAULT_RETRY_POLICY = RetryPolicy.builder()
            .withMaxRetries(5)
            .handleIf(RetryingHttpClient::isRetryableException)
            .onRetry(e -> log.warn("Retrying failed request, exception %s", e.getLastException()))
            .onRetriesExceeded(e -> log.error(e.getException(), "Failed to execute http request, retries exceeded"))
            .onFailure(e -> log.error(e.getException(), "Failed to execute http request"))
            .onSuccess(e -> {
                if (!e.isFirstAttempt()) {
                    log.info("Retry succeeded on try: %s", e.getAttemptCount());
                }
            })
            .build();

    public static final RetryPolicy<Object> TIMEOUT_RETRY_POLICY = RetryPolicy.builder()
            .withMaxRetries(5)
            .handleIf(throwable -> getCausalChain(throwable).stream().anyMatch(e -> (e instanceof TimeoutException)))
            .onRetry(e -> log.warn("Retrying failed request, exception %s", e.getLastException()))
            .onRetriesExceeded(e -> log.error(e.getException(), "Failed to execute http request, retries exceeded"))
            .onFailure(e -> log.error(e.getException(), "Failed to execute http request"))
            .build();

    private final HttpClient httpClient;
    private final RetryPolicy<Object> retryPolicy;
    private final Optional<HttpStatusListener> httpStatusListener;

    public static class RetryableException
            extends RuntimeException
    {
        public RetryableException(String message)
        {
            super(message);
        }
    }

    public RetryingHttpClient(HttpClient httpClient)
    {
        this(httpClient, DEFAULT_RETRY_POLICY, Optional.empty());
    }

    public RetryingHttpClient(HttpClient httpClient, RetryPolicy<Object> retryPolicy)
    {
        this(httpClient, retryPolicy, Optional.empty());
    }

    private RetryingHttpClient(HttpClient httpClient, RetryPolicy<Object> retryPolicy, Optional<HttpStatusListener> httpStatusListener)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.retryPolicy = requireNonNull(retryPolicy, "retryPolicy is null");
        this.httpStatusListener = requireNonNull(httpStatusListener, "httpStatusListener is null");
    }

    public RetryingHttpClient withHttpStatusListener(HttpStatusListener httpStatusListener)
    {
        return new RetryingHttpClient(httpClient, retryPolicy, Optional.of(httpStatusListener));
    }

    public RetryingHttpClient withMaxRetries(int maxRetries)
    {
        RetryPolicy<Object> retryPolicy = RetryPolicy.builder(this.retryPolicy.getConfig()).withMaxRetries(maxRetries).build();
        return new RetryingHttpClient(httpClient, retryPolicy, httpStatusListener);
    }

    public RetryingHttpClient withRetryPolicy(RetryPolicy retryPolicy)
    {
        return new RetryingHttpClient(httpClient, retryPolicy, httpStatusListener);
    }

    public static boolean isRetryableException(Throwable t)
    {
        // EOFException should be retried as the remote service likely crashed, was replaced, etc.
        return getCausalChain(t).stream().anyMatch(e -> (e instanceof RetryableException)
                || (e instanceof EOFException)
                || (e instanceof TimeoutException && e.getMessage() != null && e.getMessage().startsWith("DNS timeout ")));
    }

    @Override
    public <T, E extends Exception> T execute(Request request, ResponseHandler<T, E> responseHandler)
    {
        return Failsafe.with(retryPolicy).get(() -> {
            ResponseHandler<T, E> localResponseHandler = httpStatusListener.map(listener -> wrapWithListener(responseHandler, listener)).orElse(responseHandler);
            return httpClient.execute(request, localResponseHandler);
        });
    }

    @Override
    public <T, E extends Exception> HttpResponseFuture<T> executeAsync(Request request, ResponseHandler<T, E> responseHandler)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public StreamingResponse executeStreaming(Request request)
    {
        return Failsafe.with(retryPolicy).get(() -> httpClient.executeStreaming(request));
    }

    @Override
    public RequestStats getStats()
    {
        return httpClient.getStats();
    }

    @Override
    @PreDestroy
    public void close()
    {
        httpClient.close();
    }

    @Override
    public boolean isClosed()
    {
        return httpClient.isClosed();
    }

    private <T, E extends Exception> ResponseHandler<T, E> wrapWithListener(ResponseHandler<T, E> responseHandler, HttpStatusListener httpStatusListener)
    {
        return new ResponseHandler<>()
        {
            @Override
            public T handleException(Request request, Exception exception)
                    throws E
            {
                return responseHandler.handleException(request, exception);
            }

            @Override
            public T handle(Request request, Response response)
                    throws E
            {
                httpStatusListener.statusReceived(response.getStatusCode());
                return responseHandler.handle(request, response);
            }
        };
    }
}
