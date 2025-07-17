/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.client;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.starburst.ai.model.ModelConnectionSpecsLoader;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class FileBackedModelSpecModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(AiFileStorageConfig.class);
        binder.bind(ModelConnectionSpecsLoader.class).to(FileBackedModelConnectionSpecsLoader.class).in(Scopes.SINGLETON);
    }
}
