/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.license;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static java.util.Objects.requireNonNull;

@JsonIgnoreProperties
public class AWSIdentityDocument
{
    private final String accountId;
    private final Set<String> marketplaceProductCodes;
    private final String region;

    @JsonCreator
    public AWSIdentityDocument(
            @JsonProperty("accountId") String accountId,
            @JsonProperty("marketplaceProductCodes") Set<String> marketplaceProductCodes,
            @JsonProperty("region") String region)
    {
        this.accountId = accountId;
        this.marketplaceProductCodes = marketplaceProductCodes == null ? ImmutableSet.of() : ImmutableSet.copyOf(marketplaceProductCodes);
        this.region = requireNonNull(region, "region is null");
    }

    @JsonProperty("accountId")
    String getAccountId()
    {
        return accountId;
    }

    @JsonProperty("marketplaceProductCodes")
    Set<String> getMarketplaceProductCodes()
    {
        return marketplaceProductCodes;
    }

    @JsonProperty("region")
    String getRegion()
    {
        return region;
    }
}
