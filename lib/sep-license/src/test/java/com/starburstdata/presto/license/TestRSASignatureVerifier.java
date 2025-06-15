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

import org.junit.jupiter.api.Test;

import java.net.URL;

import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.toByteArray;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class TestRSASignatureVerifier
{
    @Test
    public void testVerifyWithOpenSSLSignature()
            throws Exception
    {
        URL publicKeyURL = getResource(getClass(), "test-public_key.der");
        SignatureVerifier verifier = new RSASignatureVerifier(publicKeyURL);

        URL licenseURL = getResource(getClass(), "test-license.json.sorted");
        URL signatureURL = getResource(getClass(), "test-license.json.signature");
        byte[] license = toByteArray(licenseURL);
        byte[] signature = toByteArray(signatureURL);
        verifier.verify(license, signature);
    }

    @Test
    public void testVerifyThrowsVerificationExceptionWhenSignatureIsWrong()
            throws Exception
    {
        URL publicKeyURL = getResource(getClass(), "test-public_key.der");
        SignatureVerifier verifier = new RSASignatureVerifier(publicKeyURL);

        URL licenseURL = getResource(getClass(), "test-license.json");
        URL signatureURL = getResource(getClass(), "test-license.json.signature");
        byte[] license = toByteArray(licenseURL);
        byte[] signature = toByteArray(signatureURL);
        assertThatExceptionOfType(VerificationException.class)
                .isThrownBy(() -> verifier.verify(license, signature));
    }
}
