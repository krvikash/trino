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

@JsonIgnoreProperties
public class ManagedKubernetesIdentityDocument
{
    private final String accountId;
    private final String status;
    private final String timestamp;
    private final String cloud;

    @JsonCreator
    public ManagedKubernetesIdentityDocument(
            @JsonProperty("accountId") String accountId,
            @JsonProperty("status") String status,
            @JsonProperty("timestamp") String timestamp,
            @JsonProperty("cloud") String cloud)
    {
        this.accountId = accountId;
        this.status = status;
        this.timestamp = timestamp;
        this.cloud = cloud;
    }

    @JsonProperty("accountId")
    String getAccountId()
    {
        return accountId;
    }

    @JsonProperty("status")
    String getStatus()
    {
        return status;
    }

    @JsonProperty("region")
    String getTimestamp()
    {
        return timestamp;
    }

    @JsonProperty("cloud")
    String getCloud()
    {
        return cloud;
    }
}
