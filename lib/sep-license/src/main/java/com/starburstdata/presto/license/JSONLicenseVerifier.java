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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteSource;
import io.airlift.json.ObjectMapperProvider;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static java.util.Objects.requireNonNull;

class JSONLicenseVerifier
{
    private static final ObjectMapperProvider OBJECT_MAPPER_PROVIDER = new ObjectMapperProvider();

    private final SignatureVerifier verifier;

    JSONLicenseVerifier()
    {
        this(new RSASignatureVerifier());
    }

    @VisibleForTesting
    JSONLicenseVerifier(SignatureVerifier verifier)
    {
        this.verifier = requireNonNull(verifier, "verifier is null");
    }

    /**
     * Reads a JSON-formatted license from an byte source and verifies it.
     *
     * @param licenseSource a byte source to read a license from.
     * @return license object if data is properly deserialized and license is valid.
     * @throws IOException if reading from a stream obtained from the byte source fails.
     * @throws VerificationException if the signature is invalid or the underlying {@link SignatureVerifier} fails.
     */
    public License verify(ByteSource licenseSource)
            throws IOException, VerificationException
    {
        try (InputStream licenseStream = licenseSource.openBufferedStream()) {
            License license = OBJECT_MAPPER_PROVIDER.get().readValue(licenseStream, License.class);
            byte[] signatureData = Base64.getDecoder().decode(license.getBase64Signature());
            String licenseStr = OBJECT_MAPPER_PROVIDER.get().writeValueAsString(license);
            byte[] licenseData = licenseStr.getBytes(StandardCharsets.UTF_8);
            verifier.verify(licenseData, signatureData);
            license.setHash(Hashing.sha256().hashBytes(licenseData).toString());
            license.setType(LicenseType.JSON);
            return license;
        }
    }
}
