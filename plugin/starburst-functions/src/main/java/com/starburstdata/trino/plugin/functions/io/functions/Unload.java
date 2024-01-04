/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.functions.io.functions;

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.starburst.functions.io.StorageConfig;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorTableFunction;
import io.trino.plugin.hive.functions.Unload.UnloadFunction;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.security.LocationAccessControl;

import static java.util.Objects.requireNonNull;

public class Unload
        implements Provider<ConnectorTableFunction>
{
    private final LocationAccessControl locationAccessControl;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final boolean useRowSemantics;

    @Inject
    public Unload(LocationAccessControl locationAccessControl, TrinoFileSystemFactory fileSystemFactory, StorageConfig config)
    {
        this.locationAccessControl = requireNonNull(locationAccessControl, "locationAccessControl is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.useRowSemantics = config.isUseRowSemantics();
    }

    @Override
    public ConnectorTableFunction get()
    {
        return new ClassLoaderSafeConnectorTableFunction(new UnloadFunction("io", locationAccessControl, fileSystemFactory, useRowSemantics), getClass().getClassLoader());
    }
}
