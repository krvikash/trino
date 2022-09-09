/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.server;

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

import java.net.URI;

public class DataServerConfig
{
    private URI discoveryServiceUri;
    private boolean includeChecksumInDataResponse = true;

    @NotNull
    public URI getDiscoveryServiceUri()
    {
        return discoveryServiceUri;
    }

    @Config("discovery-service.uri")
    public DataServerConfig setDiscoveryServiceUri(URI discoveryServiceUri)
    {
        this.discoveryServiceUri = discoveryServiceUri;
        return this;
    }

    public boolean getIncludeChecksumInDataResponse()
    {
        return includeChecksumInDataResponse;
    }

    @Config("include-checksum-in-data-response")
    public DataServerConfig setIncludeChecksumInDataResponse(boolean includeChecksumInDataResponse)
    {
        this.includeChecksumInDataResponse = includeChecksumInDataResponse;
        return this;
    }
}
