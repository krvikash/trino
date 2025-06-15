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
import static org.assertj.core.api.Assertions.assertThat;

public class TestRSASignatureGenerator
{
    @Test
    public void testGeneratedSignatureMatchesOpenSSLSignature()
            throws Exception
    {
        URL privateKeyURL = getResource(getClass(), "test-private_key.der");
        SignatureGenerator signatureGenerator = new RSASignatureGenerator(privateKeyURL, "SHA512withRSA");

        URL licenseURL = getResource(getClass(), "test-license.json.sorted");
        URL openSSLSignatureURL = getResource(getClass(), "test-license.json.signature");
        byte[] license = toByteArray(licenseURL);
        byte[] openSSLSignature = toByteArray(openSSLSignatureURL);
        byte[] ourSignature = signatureGenerator.sign(license);

        assertThat(ourSignature).as("Generated signature matches OpenSSL signature").containsExactly(openSSLSignature);
    }
}
