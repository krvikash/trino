/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.trino.exchange.ratelimiting;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.name.Named;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import io.airlift.units.Duration;
import io.starburst.stargate.buffer.data.exception.DataServerException;
import io.starburst.stargate.buffer.data.server.AddDataPagesThrottlingCalculator;
import io.starburst.stargate.buffer.data.server.DataServerConfig;
import jakarta.annotation.Nullable;
import jakarta.servlet.AsyncContext;
import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.CompletionCallback;
import jakarta.ws.rs.container.ConnectionCallback;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Verify.verify;
import static com.google.common.net.HttpHeaders.CONTENT_LENGTH;
import static com.google.inject.name.Names.named;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.units.Duration.succinctDuration;
import static io.starburst.stargate.buffer.data.client.DataClientHeaders.MAX_WAIT;
import static io.starburst.stargate.buffer.data.client.ErrorCode.INTERNAL_ERROR;
import static io.starburst.stargate.buffer.data.client.ErrorCode.OVERLOADED;
import static io.starburst.stargate.buffer.data.client.HttpDataClient.AVERAGE_PROCESS_TIME_IN_MILLIS_HEADER;
import static io.starburst.stargate.buffer.data.client.HttpDataClient.CLIENT_ID_HEADER;
import static io.starburst.stargate.buffer.data.client.HttpDataClient.ERROR_CODE_HEADER;
import static io.starburst.stargate.buffer.data.client.HttpDataClient.RATE_LIMIT_HEADER;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class RateLimitingTestServer
        implements Closeable
{
    private final Closer closer = Closer.create();
    private final URI baseUri;

    public RateLimitingTestServer(
            int maxInProgressAddDataPagesRequests,
            int inProgressAddDataPagesRequestsRateLimitThreshold,
            Duration inProgressAddDataPagesRequestsThrottlingCounterDecayDuration,
            Duration requestProcessingTime)
    {
        ImmutableList.Builder<Module> modules = ImmutableList.builder();
        modules.add(
                new TestingNodeModule("test"),
                new TestingHttpServerModule(),
                new JsonModule(),
                new JaxrsModule(),
                new AbstractConfigurationAwareModule() {
                    @Override
                    protected void setup(Binder binder)
                    {
                        configBinder(binder).bindConfig(DataServerConfig.class);
                        configBinder(binder).bindConfigDefaults(DataServerConfig.class, config -> {
                            config.setMaxInProgressAddDataPagesRequests(maxInProgressAddDataPagesRequests);
                            config.setInProgressAddDataPagesRequestsRateLimitThreshold(inProgressAddDataPagesRequestsRateLimitThreshold);
                            config.setInProgressAddDataPagesRequestsThrottlingCounterDecayDuration(inProgressAddDataPagesRequestsThrottlingCounterDecayDuration);
                        });
                        binder.bind(Duration.class).annotatedWith(named("RateLimitingTestServer.requestProcessingTime")).toInstance(requestProcessingTime);
                        binder.bind(AddDataPagesThrottlingCalculator.class).in(Scopes.SINGLETON);
                        jaxrsBinder(binder).bind(TestingDataResource.class);
                    }
                });

        Bootstrap app = new Bootstrap(modules.build());

        Injector injector = app
                .quiet()
                .doNotInitializeLogging()
                .initialize();

        LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
        closer.register(lifeCycleManager::stop);

        HttpServerInfo httpServerInfo = injector.getInstance(HttpServerInfo.class);
        baseUri = UriBuilder.fromUri(httpServerInfo.getHttpsUri() != null ? httpServerInfo.getHttpsUri() : httpServerInfo.getHttpUri())
                .host("localhost")
                .build();
    }

    public URI getBaseUri()
    {
        return baseUri;
    }

    @Override
    public void close()
            throws IOException
    {
        closer.close();
    }

    @Path("/api/v1/buffer/data")
    public static class TestingDataResource
    {
        private static final Duration CLIENT_MAX_WAIT_LIMIT = succinctDuration(60, TimeUnit.SECONDS);

        private final AddDataPagesThrottlingCalculator addDataPagesThrottlingCalculator;
        private final int maxInProgressAddDataPagesRequests;
        private final Duration requestProcessingTime;
        private final ScheduledExecutorService executorService;

        private final AtomicInteger inProgressAddDataPagesRequests = new AtomicInteger();
        private final byte[] drainBuffer = new byte[1024 * 1024];
        private AtomicLong requestCounter = new AtomicLong();

        @Inject
        public TestingDataResource(
                AddDataPagesThrottlingCalculator addDataPagesThrottlingCalculator,
                DataServerConfig config,
                @Named("RateLimitingTestServer.requestProcessingTime") Duration requestProcessingTime)
        {
            this.addDataPagesThrottlingCalculator = requireNonNull(addDataPagesThrottlingCalculator, "addDataPagesThrottlingCalculator is null");
            this.maxInProgressAddDataPagesRequests = config.getMaxInProgressAddDataPagesRequests();
            this.requestProcessingTime = requestProcessingTime;
            this.executorService = Executors.newScheduledThreadPool(4);
        }

        @POST
        @Path("{exchangeId}/addDataPages/{taskId}/{attemptId}/{dataPagesId}")
        @Consumes(MediaType.APPLICATION_OCTET_STREAM)
        public void addDataPages(
                @Context HttpServletRequest request,
                @PathParam("exchangeId") String exchangeId,
                @PathParam("taskId") int taskId,
                @PathParam("attemptId") int attemptId,
                @PathParam("dataPagesId") long dataPagesId,
                @QueryParam("targetBufferNodeId") @Nullable Long targetBufferNodeId,
                @HeaderParam(CONTENT_LENGTH) Integer contentLength,
                @HeaderParam(MAX_WAIT) Duration clientMaxWait,
                @Suspended AsyncResponse asyncResponse)
                throws IOException
        {
            long processingStart = System.currentTimeMillis();
            int currentInProgressAddDataPagesRequests = inProgressAddDataPagesRequests.incrementAndGet();
            AsyncContext asyncContext = request.getAsyncContext();
            if (currentInProgressAddDataPagesRequests > maxInProgressAddDataPagesRequests) {
                inProgressAddDataPagesRequests.decrementAndGet();
                addDataPagesThrottlingCalculator.recordThrottlingEvent();
                completeServletResponse(
                        asyncContext,
                        processingStart,
                        Optional.of(new DataServerException(OVERLOADED, "Exceeded maximum in progress addDataPages requests (%s)".formatted(maxInProgressAddDataPagesRequests))));
                return;
            }

            asyncResponse.setTimeout(getAsyncTimeout(clientMaxWait).toMillis(), MILLISECONDS);

            ServletInputStream inputStream;
            try {
                inputStream = asyncContext.getRequest().getInputStream();
            }
            catch (IOException e) {
                try {
                    completeServletResponse(asyncContext, processingStart, Optional.of(new DataServerException(INTERNAL_ERROR, "cannot get input stream", e)));
                    return;
                }
                finally {
                    inProgressAddDataPagesRequests.decrementAndGet();
                }
            }

            AtomicBoolean requestFinalizedGuard = new AtomicBoolean();
            try {
                asyncResponse.register((CompletionCallback) throwable -> {
                    if (throwable != null) {
                        finalizeRequest(request, asyncResponse, errorResponse(throwable, getRateLimitHeaders(request)), processingStart, requestFinalizedGuard);
                    }
                });

                asyncResponse.register((ConnectionCallback) response -> {
                    finalizeRequest(request, asyncResponse, errorResponse(new RuntimeException("client disconnected"), getRateLimitHeaders(request)), processingStart, requestFinalizedGuard);
                });
            }
            catch (Exception e) {
                finalizeRequest(request, asyncResponse, errorResponse(e, getRateLimitHeaders(request)), processingStart, requestFinalizedGuard);
            }

            ReadListener readListener = new ReadListener()
            {
                @Override
                public void onDataAvailable()
                        throws IOException
                {
                    while (inputStream.isReady()) {
                        int n = inputStream.read(drainBuffer);
                        if (n == -1) {
                            break;
                        }
                    }
                }

                @Override
                public void onAllDataRead()
                {
                    executorService.schedule(() -> {
                        try {
                            Response.ResponseBuilder response = Response.ok();
                            addRateLimitHeaders(request, response);
                            finalizeRequest(request, asyncResponse, response.build(), processingStart, requestFinalizedGuard);
                        }
                        catch (Throwable e) {
                            finalizeRequest(request, asyncResponse, errorResponse(e, getRateLimitHeaders(request)), processingStart, requestFinalizedGuard);
                        }
                    }, requestProcessingTime.toMillis(), MILLISECONDS);
                }

                @Override
                public void onError(Throwable throwable)
                {
                    finalizeRequest(request, asyncResponse, errorResponse(throwable, getRateLimitHeaders(request)), processingStart, requestFinalizedGuard);
                }
            };
            inputStream.setReadListener(readListener);
        }

        private void finalizeRequest(HttpServletRequest request, AsyncResponse asyncResponse, Object response, long processingStart, AtomicBoolean guard)
        {
            if (!guard.getAndSet(true)) {
                asyncResponse.resume(response);
                verify(inProgressAddDataPagesRequests.decrementAndGet() >= 0);
                recordRequest(request, processingStart);
            }
        }

        private void recordRequest(HttpServletRequest request, long processingStart)
        {
            addDataPagesThrottlingCalculator.recordProcessTimeInMillis(System.currentTimeMillis() - processingStart);
            addDataPagesThrottlingCalculator.updateCounterStat(getClientId(request), 1);
        }

        private void completeServletResponse(AsyncContext asyncContext, long processingStart, Optional<DataServerException> dataServerException)
                throws IOException
        {
            HttpServletResponse servletResponse = (HttpServletResponse) asyncContext.getResponse();
            servletResponse.setContentType("text/plain");
            getRateLimitHeaders(asyncContext.getRequest()).forEach(servletResponse::setHeader);

            if (dataServerException.isPresent()) {
                servletResponse.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                servletResponse.getWriter().write(dataServerException.get().getMessage());
                servletResponse.setHeader(ERROR_CODE_HEADER, dataServerException.get().getErrorCode().toString());
            }
            else {
                servletResponse.setStatus(HttpServletResponse.SC_OK);
            }

            recordRequest((HttpServletRequest) asyncContext.getRequest(), processingStart);
            asyncContext.complete();
        }

        private Map<String, String> getRateLimitHeaders(ServletRequest request)
        {
            String clientId = getClientId((HttpServletRequest) request);
            OptionalDouble rateLimit = addDataPagesThrottlingCalculator.getRateLimit(clientId, inProgressAddDataPagesRequests.get());
            if (rateLimit.isPresent()) {
                ImmutableMap<String, String> headers = ImmutableMap.of(
                        RATE_LIMIT_HEADER, Double.toString(rateLimit.getAsDouble()),
                        AVERAGE_PROCESS_TIME_IN_MILLIS_HEADER, Long.toString(addDataPagesThrottlingCalculator.getAverageProcessTimeInMillis()));
                return headers;
            }
            return ImmutableMap.of();
        }

        private void addRateLimitHeaders(ServletRequest request, Response.ResponseBuilder responseBuilder)
        {
            Map<String, String> rateLimitHeaders = getRateLimitHeaders(request);
            for (Map.Entry<String, String> entry : rateLimitHeaders.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                responseBuilder.header(key, value);
            }
        }

        private static String getClientId(HttpServletRequest request)
        {
            String clientId = request.getHeader(CLIENT_ID_HEADER);
            if (clientId == null) {
                clientId = request.getRemoteHost();
            }
            return clientId;
        }

        private static Response errorResponse(Throwable throwable, Map<String, String> headers)
        {
            Response.ResponseBuilder responseBuilder;
            if (throwable instanceof DataServerException dataServerException) {
                responseBuilder = Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                        .header(ERROR_CODE_HEADER, dataServerException.getErrorCode())
                        .entity(throwable.getMessage());
            }
            else {
                responseBuilder = Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                        .header(ERROR_CODE_HEADER, INTERNAL_ERROR)
                        .entity(throwable.getMessage());
            }

            headers.forEach(responseBuilder::header);
            return responseBuilder.build();
        }

        Duration getAsyncTimeout(@Nullable Duration clientMaxWait)
        {
            if (clientMaxWait == null || clientMaxWait.toMillis() == 0 || clientMaxWait.compareTo(CLIENT_MAX_WAIT_LIMIT) > 0) {
                return CLIENT_MAX_WAIT_LIMIT;
            }
            return succinctDuration(clientMaxWait.toMillis() * 0.95, MILLISECONDS);
        }
    }
}
