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

import io.airlift.spi.secrets.SecretProvider;

import java.util.Map;

public class TestingSecretsProvider
        implements SecretProvider
{
    Map<String, String> secrets;

    public TestingSecretsProvider(Map<String, String> secrets)
    {
        this.secrets = secrets;
    }

    @Override
    public String resolveSecretValue(String key)
    {
        return secrets.get(key);
    }
}
