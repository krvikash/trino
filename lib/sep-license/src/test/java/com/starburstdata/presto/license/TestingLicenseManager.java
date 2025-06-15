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

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

public final class TestingLicenseManager
        implements LicenseManager
{
    public static final LicenseManager NOOP_LICENSE_MANAGER = new TestingLicenseManager(feature -> true);

    private final Predicate<StarburstFeature> hasLicense;
    private final Optional<ByteSource> fileHandle;

    public TestingLicenseManager(Predicate<StarburstFeature> hasLicense)
    {
        this(hasLicense, Optional.empty());
    }

    public TestingLicenseManager(Predicate<StarburstFeature> hasLicense, Optional<ByteSource> fileHandle)
    {
        this.hasLicense = requireNonNull(hasLicense, "hasLicense is null");
        this.fileHandle = requireNonNull(fileHandle, "fileHandle is null");
    }

    @Override
    public boolean hasLicense()
    {
        return hasLicense.test(null);
    }

    @Override
    public void checkLicense()
    {
        if (!hasFeature(null)) {
            throw new StarburstLicenseException("Not licensed");
        }
    }

    @Override
    public boolean hasFeature(StarburstFeature feature)
    {
        return hasLicense.test(feature);
    }

    @Override
    public void checkFeature(StarburstFeature feature)
    {
        requireNonNull(feature, "feature is null");
        if (!hasFeature(feature)) {
            throw new StarburstLicenseException("Not licensed: " + feature.getFeatureName());
        }
    }

    @Override
    public Optional<String> getOwner()
    {
        return Optional.of("testing");
    }

    @Override
    public LicenseType getType()
    {
        return LicenseType.UNKNOWN;
    }

    @Override
    public Optional<LocalDateTime> getExpiry()
    {
        return Optional.of(LocalDateTime.MAX);
    }

    @Override
    public Optional<ByteSource> getLicenseFileHandle()
    {
        return fileHandle;
    }
}
