/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.oracle;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;

public class UserPasswordConnectionProviderModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        newOptionalBinder(binder, OracleConnectionProvider.class).setBinding().to(PasswordAuthenticationConnectionProvider.class).in(Scopes.SINGLETON);
    }
}
