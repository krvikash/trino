/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.functions.io;

import com.google.common.collect.ImmutableList;

import java.util.List;

public record StorageConfigurations(List<StorageConfiguration> configurations)
{
    public StorageConfigurations
    {
        configurations = ImmutableList.copyOf(configurations);
    }
}
