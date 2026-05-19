/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.license;

import io.trino.spi.TrinoException;

interface SignatureGenerator
{
    /**
     * Generates a signature for a given license.
     *
     * @param license the license bytes to verify.
     * @return signature bytes.
     * @throws TrinoException if the public key for verifying signatures is invalid,
     *         the signature object is not initialized properly, if this signature algorithm
     *         is unable to process the input data provided, etc.
     */
    byte[] sign(byte[] license)
            throws TrinoException;
}
