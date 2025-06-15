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
import com.google.common.io.ByteSource;
import com.google.common.io.MoreFiles;
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static java.util.Objects.requireNonNull;

class JSONLicenseProvider
        implements LicenseProvider
{
    public static final Path LICENSE_PATH = Paths.get("etc/starburstdata.license");
    private static Logger log = Logger.get(JSONLicenseProvider.class);
    private final Path licensePath;
    private final JSONLicenseVerifier licenseVerifier;

    public JSONLicenseProvider()
    {
        this(LICENSE_PATH, new JSONLicenseVerifier());
    }

    @VisibleForTesting
    JSONLicenseProvider(Path licensePath, JSONLicenseVerifier licenseVerifier)
    {
        this.licensePath = requireNonNull(licensePath, "licensePath is null");
        this.licenseVerifier = requireNonNull(licenseVerifier, "licenseVerifier is null");
    }

    @Override
    public Optional<License> getLicense()
    {
        if (!Files.exists(licensePath)) {
            log.info("Starburst Enterprise license file does not exist: %s", licensePath);
            return Optional.empty();
        }

        try {
            ByteSource licenseSource = MoreFiles.asByteSource(licensePath);
            License license = licenseVerifier.verify(licenseSource);
            return Optional.of(license);
        }
        catch (IOException e) {
            String message = String.format("Problem encountered while opening/reading license file path: %s", licensePath);
            throw new TrinoException(CONFIGURATION_INVALID, message, e);
        }
    }

    @Override
    public Optional<ByteSource> getFileHandle()
    {
        if (Files.exists(licensePath)) {
            return Optional.of(MoreFiles.asByteSource(licensePath));
        }
        return Optional.empty();
    }
}
