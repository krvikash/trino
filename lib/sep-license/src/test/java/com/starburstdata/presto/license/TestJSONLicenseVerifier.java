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

import com.google.common.io.ByteSource;
import com.google.common.io.Resources;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static com.google.common.io.Resources.getResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class TestJSONLicenseVerifier
{
    private static final String SIGNATURE = Base64.getEncoder().encodeToString("Signature".getBytes(StandardCharsets.UTF_8));
    private static final String LICENSE_JSON = "{\"base64Signature\":\"" + SIGNATURE + "\",\"expiry\":\"2018-11-13T15:30:27\"," +
            "\"features\":[\"c\",\"b\",\"a\"],\"owner\":\"Marie-hélène de Mendoza <mdemendoza@test.starburstdata.net>\"}";
    private static final String LICENSE_JSON_WITHOUT_SIGNATURE = "{\"expiry\":\"2018-11-13T15:30:27\"," +
            "\"features\":[\"a\",\"b\",\"c\"],\"owner\":\"Marie-hélène de Mendoza <mdemendoza@test.starburstdata.net>\"}";
    private static final String GIBBERISH = "fslkgjoifgjoivèbjuobiu450s87u098fguxoplbijo3iu4j908ufdodfhlkdo";

    @Test
    public void testParseCompleteLicense()
            throws IOException
    {
        JSONLicenseVerifier verifier = new JSONLicenseVerifier(TestJSONLicenseVerifier::signatureVerifier);
        License license = verifier.verify(ByteSource.wrap(LICENSE_JSON.getBytes(StandardCharsets.UTF_8)));

        assertThat(license.getOwner()).isEqualToNormalizingNewlines("Marie-hélène de Mendoza <mdemendoza@test.starburstdata.net>");
        assertThat(license.getExpiry()).isEqualTo("2018-11-13T15:30:27");
        assertThat(license.getFeatures()).containsExactly("a", "b", "c");
        assertThat(license.getBase64Signature()).isEqualTo(SIGNATURE);
    }

    @Test
    public void testThrowIfSignatureMissing()
    {
        JSONLicenseVerifier verifier = new JSONLicenseVerifier(TestJSONLicenseVerifier::signatureVerifier);
        assertThatExceptionOfType(IOException.class)
                .isThrownBy(() -> verifier.verify(ByteSource.wrap(LICENSE_JSON_WITHOUT_SIGNATURE.getBytes(StandardCharsets.UTF_8))))
                .withMessageStartingWith("Cannot construct instance of `com.starburstdata.presto.license.License`, problem: base64Signature is null");
    }

    @Test
    public void testThrowIfLicenseMalformed()
    {
        JSONLicenseVerifier verifier = new JSONLicenseVerifier(TestJSONLicenseVerifier::signatureVerifier);
        assertThatExceptionOfType(IOException.class)
                .isThrownBy(() -> verifier.verify(ByteSource.wrap(GIBBERISH.getBytes(StandardCharsets.UTF_8))))
                .withMessageStartingWith(String.format("Unrecognized token '%s'", GIBBERISH));
    }

    @Test
    public void testVerifyOpenSSLSignedLicense()
            throws Exception
    {
        JSONLicenseVerifier jsonLicenseVerifier = new JSONLicenseVerifier(TestJSONLicenseVerifier::signatureVerifier);
        URL testLicenseURL = getResource(getClass(), "test-license.json.signed");
        License testLicense = jsonLicenseVerifier.verify(Resources.asByteSource(testLicenseURL));

        assertThat(testLicense.getOwner()).isEqualToNormalizingNewlines("Marie-hélène de Mendoza <mdemendoza@test.starburstdata.net>");
        assertThat(testLicense.getExpiry()).isEqualTo("2000-01-01T00:00:00");
        assertThat(testLicense.getFeatures()).containsExactly("constantTimePrimeFactorization", "ranger", "sentry");
        assertThat(testLicense.getHash().orElse("missing")).isEqualTo("82480a557f4e72e805a65ea0ea13b32039feeae312dbc60dd9b5c740c22191d1");
    }

    private static void signatureVerifier(byte[] license, byte[] signature)
    {
        // do nothing
    }
}
