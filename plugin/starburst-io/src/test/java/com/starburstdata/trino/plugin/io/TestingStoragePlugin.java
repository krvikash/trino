/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.io;

import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import io.trino.spi.connector.ConnectorFactory;

import java.util.List;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class TestingStoragePlugin
        extends StoragePlugin
{
    private final Module module;

    public TestingStoragePlugin(Module module)
    {
        this.module = requireNonNull(module, "module is null");
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        List<ConnectorFactory> connectorFactories = ImmutableList.copyOf(super.getConnectorFactories());
        verify(connectorFactories.size() == 1, "Unexpected connector factories: %s", connectorFactories);

        return ImmutableList.of(new StorageConnectorFactory(module));
    }
}
