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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.net.URL;

import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.toByteArray;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestRSASignaturePipeline
{
    private URL privateKeyURL;
    private SignatureGenerator signatureGenerator;
    private URL publicKeyURL;
    private SignatureVerifier signatureVerifier;
    private byte[] licenseData;

    @BeforeAll
    public void setup()
            throws Exception
    {
        privateKeyURL = getResource(getClass(), "test-private_key.der");
        signatureGenerator = new RSASignatureGenerator(privateKeyURL);
        publicKeyURL = getResource(getClass(), "test-public_key.der");
        signatureVerifier = new RSASignatureVerifier(publicKeyURL);
        licenseData = toByteArray(getResource(getClass(), "test-license.json"));
    }

    @Test
    public void testPipelineSuccessfulWithDefaultAlgorithms()
    {
        byte[] signatureData = signatureGenerator.sign(licenseData);
        signatureVerifier.verify(licenseData, signatureData);
    }

    @Test
    public void testPipelineSuccessfulWithMatchedAlgorithms()
            throws Exception
    {
        SignatureGenerator sha1SignatureGenerator = new RSASignatureGenerator(privateKeyURL, "SHA1withRSA");
        byte[] signatureData = sha1SignatureGenerator.sign(licenseData);
        SignatureVerifier sha1SignatureVerifier = new RSASignatureVerifier(publicKeyURL, "SHA1withRSA");
        sha1SignatureVerifier.verify(licenseData, signatureData);
    }

    @Test
    public void testPipelineFailsWithUnmatchedAlgorithms()
    {
        byte[] signatureData = signatureGenerator.sign(licenseData);
        SignatureVerifier sha1SignatureVerifier = new RSASignatureVerifier(publicKeyURL, "SHA1withRSA");
        assertThatExceptionOfType(VerificationException.class)
                .isThrownBy(() -> sha1SignatureVerifier.verify(licenseData, signatureData))
                .withMessage("No valid license found");
    }
}
