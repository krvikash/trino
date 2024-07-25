/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.saphana;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotNull;

import static com.starburstdata.trino.plugin.saphana.SapHanaParallelismType.NO_PARALLELISM;

public class SapHanaConfig
{
    private SapHanaParallelismType parallelismType = NO_PARALLELISM;

    @NotNull
    public SapHanaParallelismType getParallelismType()
    {
        return parallelismType;
    }

    @Config("sap-hana.parallelism-type")
    @ConfigDescription("Concurrency strategy for reads")
    public SapHanaConfig setParallelismType(SapHanaParallelismType parallelismType)
    {
        this.parallelismType = parallelismType;
        return this;
    }
}
