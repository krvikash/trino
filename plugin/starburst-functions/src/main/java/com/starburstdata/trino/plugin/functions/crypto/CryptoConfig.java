/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.functions.crypto;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

public class CryptoConfig
{
    private boolean enabled;

    public boolean isEnabled()
    {
        return enabled;
    }

    @Config("crypto.enabled")
    @ConfigDescription("Expose the AES encryption and decryption functions in the crypto schema")
    public CryptoConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }
}
