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

interface InstanceIdentityVerifier
{
    /**
     * Verifies an instance identity document with its signature.
     *
     * @param identity the instance identity document bytes to verify.
     * @param base64Signature the BASE64 encoded signature to verify with.
     * @throws VerificationException if the signature object is not initialized properly, the passed-in signature is improperly
     * encoded or of the wrong type, if this signature algorithm is unable to
     * process the input data provided, etc.
     */
    void verify(byte[] identity, byte[] base64Signature)
            throws VerificationException;
}
