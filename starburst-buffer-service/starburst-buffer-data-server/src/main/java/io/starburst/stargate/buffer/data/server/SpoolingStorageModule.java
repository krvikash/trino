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

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.starburst.stargate.buffer.data.execution.SpoolingDirectoryConfig;
import io.starburst.stargate.buffer.data.spooling.trinofs.TrinoFsSpoolingStorageModule;

import java.util.Optional;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class SpoolingStorageModule
        extends AbstractConfigurationAwareModule
{
    private final Optional<String> configPrefix;
    private final boolean bindConfigsOnly;

    public SpoolingStorageModule(Optional<String> configPrefix, boolean bindConfigsOnly)
    {
        this.configPrefix = requireNonNull(configPrefix, "configPrefix is null");
        this.bindConfigsOnly = bindConfigsOnly;
    }

    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(SpoolingDirectoryConfig.class, configPrefix.orElse(null));
        SpoolingDirectoryConfig spoolingDirectoryConfig = buildConfigObject(SpoolingDirectoryConfig.class, configPrefix.orElse(null));
        switch (spoolingDirectoryConfig.getStorageDriver()) {
            case NATIVE -> install(new NativeSpoolingStorageModule(configPrefix, bindConfigsOnly));
            case TRINO_FS -> install(new TrinoFsSpoolingStorageModule(configPrefix, bindConfigsOnly));
        }
    }
}
