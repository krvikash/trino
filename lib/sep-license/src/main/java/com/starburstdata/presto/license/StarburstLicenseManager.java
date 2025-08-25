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
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

class StarburstLicenseManager
        implements LicenseManager
{
    private final Clock clock;
    private final Supplier<LicenseHolder> holder;

    public StarburstLicenseManager(Set<LicenseProvider> licenseProviders)
    {
        this(Clock.systemDefaultZone(), licenseProviders);
    }

    @VisibleForTesting
    StarburstLicenseManager(Clock clock, Set<LicenseProvider> licenseProviders)
    {
        this.clock = clock;
        this.holder = Suppliers.memoize(findLicense(ImmutableList.copyOf(requireNonNull(requireNonNull(licenseProviders), "requireNonNull(licenseProviders) is null")))::get);
    }

    private static Supplier<LicenseHolder> findLicense(List<LicenseProvider> licenseProviders)
    {
        return () -> licenseProviders.stream()
                .map(licenseProvider -> new LicenseHolder(licenseProvider.getLicense(), licenseProvider.getFileHandle()))
                .filter(licenseHolder -> licenseHolder.license.isPresent())
                .findFirst()
                .orElse(LicenseHolder.EMPTY_HOLDER);
    }

    private Optional<License> getLicense()
    {
        return holder.get().license;
    }

    @Override
    public boolean hasLicense()
    {
        return checkFeature(Optional.empty(), _ -> {});
    }

    @Override
    public void checkLicense()
    {
        boolean hasLicense = checkFeature(Optional.empty(), message -> {
            throw new StarburstLicenseException(message);
        });
        verify(hasLicense, "Not licensed for feature but check did not throw");
    }

    @Override
    public boolean hasFeature(StarburstFeature feature)
    {
        return checkFeature(Optional.of(feature), _ -> {});
    }

    @Override
    public void checkFeature(StarburstFeature feature)
    {
        requireNonNull(feature, "feature is null");
        @SuppressWarnings("FormatStringAnnotation") // the lambda is implementing a @FormatMethod, but Error Prone doesn't see it somehow
        boolean hasLicense = checkFeature(Optional.of(feature), (message) -> {
            throw new StarburstLicenseException(message);
        });
        verify(hasLicense, "Not licensed for feature but check did not throw");
    }

    private boolean checkFeature(Optional<StarburstFeature> feature, ErrorReporter errorReporter)
    {
        Optional<License> license = getLicense();
        if (license.isEmpty()) {
            errorReporter.report(feature.map(StarburstFeature::getDisplayName)
                    .map("Valid license required to use the feature: %s"::formatted)
                    .orElse("Starburst Enterprise requires valid license"));
            return false;
        }

        LocalDateTime now = LocalDateTime.now(clock);
        if (license.get().getExpiry().isBefore(now)) {
            errorReporter.report("The license expired on: %s, current local time is: %s".formatted(license.get().getExpiry(), now));
            return false;
        }

        if (feature.isEmpty()) {
            return true;
        }

        // avoid looking up features by name, since there are a lot of licenses that have references to non-existent features
        Set<String> eligibleFeaturesNames = Arrays.stream(StarburstFeature.values())
                .filter(f -> f.effectiveFeatures().contains(feature.get()))
                .map(StarburstFeature::getFeatureName)
                .collect(toImmutableSet());
        if (license.get().getFeatures().stream().anyMatch(eligibleFeaturesNames::contains)) {
            return true;
        }

        errorReporter.report("Not licensed for feature " + feature.get().getDisplayName());
        return false;
    }

    @Override
    public Optional<String> getOwner()
    {
        return getLicense().map(License::getOwner);
    }

    @Override
    public LicenseType getType()
    {
        return getLicense().map(License::getType).orElse(LicenseType.UNKNOWN);
    }

    @Override
    public Optional<LocalDateTime> getExpiry()
    {
        return getLicense().map(License::getExpiry);
    }

    @Override
    public Optional<ByteSource> getLicenseFileHandle()
    {
        return holder.get().fileHandle;
    }

    @Override
    public Optional<String> getHash()
    {
        return getLicense().flatMap(License::getHash);
    }

    private static class LicenseHolder
    {
        protected static final LicenseHolder EMPTY_HOLDER = new LicenseHolder(Optional.empty(), Optional.empty());

        private final Optional<License> license;
        private final Optional<ByteSource> fileHandle;

        public LicenseHolder(Optional<License> license, Optional<ByteSource> fileHandle)
        {
            this.license = requireNonNull(license);
            this.fileHandle = requireNonNull(fileHandle);
        }
    }

    @FunctionalInterface
    private interface ErrorReporter
    {
        void report(String message);
    }
}
