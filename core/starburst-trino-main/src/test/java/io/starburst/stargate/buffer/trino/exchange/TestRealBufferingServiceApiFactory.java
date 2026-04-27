/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.trino.exchange;

import io.starburst.stargate.buffer.BufferNodeInfo;
import io.starburst.stargate.buffer.BufferNodeState;
import io.starburst.stargate.buffer.data.client.DataApiFactory;
import io.starburst.stargate.buffer.discovery.client.BufferNodeInfoResponse;
import io.starburst.stargate.buffer.discovery.client.DiscoveryApi;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class TestRealBufferingServiceApiFactory
{
    private static final URI BASE_URI = URI.create("http://node1:8080");
    private static final URI VIRTUAL_THREADS_URI = URI.create("http://node1:8096");
    private static final long NODE_ID = 1L;

    @Test
    void testUsesBaseUriWhenVirtualThreadsDisabled()
    {
        AtomicReference<URI> capturedUri = new AtomicReference<>();
        RealBufferingServiceApiFactory factory = createFactory(false, capturedUri);

        factory.createDataApi(bufferNodeInfo(Optional.of(VIRTUAL_THREADS_URI)));

        // Even if the virtual threads URI is present, it should be ignored when the feature is disabled
        assertThat(capturedUri.get()).isEqualTo(BASE_URI);
    }

    @Test
    void testUsesVirtualThreadsUriWhenEnabled()
    {
        AtomicReference<URI> capturedUri = new AtomicReference<>();
        RealBufferingServiceApiFactory factory = createFactory(true, capturedUri);

        factory.createDataApi(bufferNodeInfo(Optional.of(VIRTUAL_THREADS_URI)));

        // When the feature is enabled and the virtual threads URI is present, it should be used instead of the base URI
        assertThat(capturedUri.get()).isEqualTo(VIRTUAL_THREADS_URI);
    }

    @Test
    void testFallsBackToBaseUriWhenVirtualThreadsEnabledButAbsent()
    {
        AtomicReference<URI> capturedUri = new AtomicReference<>();
        RealBufferingServiceApiFactory factory = createFactory(true, capturedUri);

        // No virtual threads URI provided
        factory.createDataApi(bufferNodeInfo(Optional.empty()));

        // When the feature is enabled but the virtual threads URI is absent, it should fall back to the base URI
        assertThat(capturedUri.get()).isEqualTo(BASE_URI);
    }

    private static RealBufferingServiceApiFactory createFactory(boolean useVirtualThreadsUri, AtomicReference<URI> capturedUri)
    {
        DataApiFactory dataApiFactory = (baseUri, _) -> {
            capturedUri.set(baseUri);
            return null;
        };
        BufferExchangeConfig config = new BufferExchangeConfig()
                .setUseVirtualThreadsUri(useVirtualThreadsUri);
        DiscoveryApi discoveryApi = new DiscoveryApi() {
            @Override
            public void updateBufferNode(BufferNodeInfo bufferNodeInfo)
            {
            }

            @Override
            public BufferNodeInfoResponse getBufferNodes()
            {
                return null;
            }
        };
        return new RealBufferingServiceApiFactory(discoveryApi, dataApiFactory, config);
    }

    private static BufferNodeInfo bufferNodeInfo(Optional<URI> virtualThreadsUri)
    {
        return new BufferNodeInfo(NODE_ID, BASE_URI, virtualThreadsUri, Optional.empty(), BufferNodeState.ACTIVE, Instant.now());
    }
}
