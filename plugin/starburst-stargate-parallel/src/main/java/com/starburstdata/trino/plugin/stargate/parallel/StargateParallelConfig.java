/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.stargate.parallel;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigHidden;
import io.trino.client.spooling.encoding.QueryDataDecoders;
import jakarta.validation.constraints.AssertTrue;

public class StargateParallelConfig
{
    private String encoding = "json+zstd";

    public String getEncoding()
    {
        return encoding;
    }

    @ConfigHidden
    @ConfigDescription("Spooled protocol encoding used while retrieving data from the remote cluster")
    @Config("encoding")
    public StargateParallelConfig setEncoding(String encoding)
    {
        this.encoding = encoding;
        return this;
    }

    @AssertTrue(message = "Provided spooled protocol encoding doesn't exist")
    public boolean encodingIsValid()
    {
        return QueryDataDecoders.exists(encoding);
    }
}
