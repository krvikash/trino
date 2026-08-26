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

import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

final class AesKeys
{
    static final int BLOCK_LENGTH = 16;

    private AesKeys() {}

    /**
     * Builds an AES key from raw key material, accepting only the three key lengths AES defines.
     * Key material of any other length is rejected rather than stretched or truncated, so that a
     * misconfigured key fails the query instead of silently encrypting under a derived key.
     */
    static SecretKey secretKey(Slice key)
    {
        int length = key.length();
        if (length != 16 && length != 24 && length != 32) {
            throw new TrinoException(
                    INVALID_FUNCTION_ARGUMENT,
                    "Key must be 16, 24 or 32 bytes long for AES-128, AES-192 or AES-256, but is %s bytes long".formatted(length));
        }
        return new SecretKeySpec(key.byteArray(), key.byteArrayOffset(), length, "AES");
    }
}
