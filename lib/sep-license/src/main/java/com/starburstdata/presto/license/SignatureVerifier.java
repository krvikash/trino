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

interface SignatureVerifier
{
    /**
     * Verifies a license with its signature.
     *
     * @param license the license bytes to verify.
     * @param signature the signature bytes to verify with.
     * @throws VerificationException if the signature is invalid,
     * the signature object is not initialized properly, the passed-in signature is improperly
     * encoded or of the wrong type, if this signature algorithm is unable to
     * process the input data provided, etc.
     */
    void verify(byte[] license, byte[] signature)
            throws VerificationException;
}
