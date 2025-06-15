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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.google.common.io.Resources.getResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestJSONLicenseProvider
{
    private static final URL TEST_PUBLIC_KEY = getResource(TestJSONLicenseProvider.class, "test-public_key.der");
    private static final JSONLicenseVerifier JSON_LICENSE_VERIFIER = new JSONLicenseVerifier(new RSASignatureVerifier(TEST_PUBLIC_KEY));

    private Path testLicensePath;

    @BeforeAll
    public void setUp()
            throws URISyntaxException
    {
        testLicensePath = Paths.get(getResource(TestJSONLicenseProvider.class, "test-license.json.signed").toURI());
    }

    @Test
    public void testReturnsEmptyOptionWhenLicensePathNotSet()
    {
        JSONLicenseProvider jsonLicenseProvider = new JSONLicenseProvider(Paths.get("/this-file-does-not-exist-never-ever"), JSON_LICENSE_VERIFIER);
        assertThat(jsonLicenseProvider.getLicense()).isEmpty();
        assertThat(jsonLicenseProvider.getFileHandle()).isEmpty();
    }

    @Test
    public void testPropagatesVerificationException()
    {
        JSONLicenseVerifier jsonLicenseVerifier = new JSONLicenseVerifier(new RSASignatureVerifier(getResource(getClass(), "presto-license_public_key.der")));
        JSONLicenseProvider jsonLicenseProvider = new JSONLicenseProvider(testLicensePath, jsonLicenseVerifier);
        assertThatExceptionOfType(TrinoException.class)
                .isThrownBy(jsonLicenseProvider::getLicense)
                .withMessage("No valid license found");
    }

    @Test
    public void testReturnsLicenseWhenVerifierSucceeds()
            throws Exception
    {
        JSONLicenseProvider jsonLicenseProvider = new JSONLicenseProvider(testLicensePath, JSON_LICENSE_VERIFIER);
        assertThat(jsonLicenseProvider.getLicense()).isPresent();

        var fileHandle = jsonLicenseProvider.getFileHandle();
        assertThat(fileHandle).isPresent();
        assertThat(fileHandle.get().openStream().readAllBytes()).isEqualTo(Files.readAllBytes(testLicensePath));
    }
}
