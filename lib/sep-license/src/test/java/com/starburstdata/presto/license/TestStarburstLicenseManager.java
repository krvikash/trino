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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Optional;

import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class TestStarburstLicenseManager
{
    private static final Clock PAST = Clock.fixed(Instant.EPOCH, UTC);
    private static final Clock FUTURE = Clock.fixed(Instant.parse("2525-01-01T10:15:30.00Z"), UTC);
    private static final StarburstFeature A_FEATURE = StarburstFeature.WARP_SPEED;

    private static Optional<License> getTestLicense()
    {
        return Optional.of(License.unsigned("testing", LicenseType.UNKNOWN, LocalDateTime.parse("2000-01-01T00:00:00"), ImmutableSortedSet.of("bigquery", "ranger", "sentry")));
    }

    private static Optional<License> getWildcardLicense()
    {
        return Optional.of(License.unsignedAllFeatures("testing", LicenseType.UNKNOWN, LocalDateTime.parse("2000-01-01T00:00:00")));
    }

    private static Optional<License> getExplicitLicense(StarburstFeature feature)
    {
        return Optional.of(License.unsigned("testing", LicenseType.UNKNOWN, LocalDateTime.parse("2000-01-01T00:00:00"), ImmutableSortedSet.of("*", feature.getFeatureName())));
    }

    private static Optional<License> failure()
    {
        throw new AssertionError("This function should never be called");
    }

    @Test
    public void testObtainsLicenseFromSecondProviderIfFirstProviderReturnsEmptyOption()
    {
        LicenseManager licenseManager = new StarburstLicenseManager(PAST, ImmutableSet.of(Optional::empty, TestStarburstLicenseManager::getTestLicense));
        licenseManager.checkLicense();
    }

    @Test
    public void testObtainsLicenseFromSecondProviderOnlyIfFirstProviderReturnsEmptyOption()
    {
        LicenseManager licenseManager = new StarburstLicenseManager(PAST, ImmutableSet.of(TestStarburstLicenseManager::getTestLicense, TestStarburstLicenseManager::failure));
        licenseManager.checkLicense();
    }

    @Test
    public void testGenericLicenseIsEnabledBeforeExpiry()
    {
        LicenseManager licenseManager = new StarburstLicenseManager(PAST, ImmutableSet.of(TestStarburstLicenseManager::getTestLicense));
        licenseManager.checkLicense();
    }

    @ParameterizedTest
    @EnumSource(StarburstFeature.class)
    public void testExplicitFeature(StarburstFeature feature)
    {
        LicenseManager licenseManager = new StarburstLicenseManager(PAST, ImmutableSet.of(TestStarburstLicenseManager::getWildcardLicense));
        assertThatExceptionOfType(StarburstLicenseException.class)
                .isThrownBy(() -> licenseManager.checkFeature(feature))
                .withMessage("License does not allow to use the feature: " + feature.getDisplayName());
    }

    @ParameterizedTest
    @EnumSource(StarburstFeature.class)
    public void testFeature(StarburstFeature feature)
    {
        LicenseManager licenseManager = new StarburstLicenseManager(PAST, ImmutableSet.of(() -> getExplicitLicense(feature)));
        licenseManager.checkFeature(feature);
    }

    @Test
    public void testLicenseProperties()
    {
        LicenseManager licenseManager = new StarburstLicenseManager(PAST, ImmutableSet.of(TestStarburstLicenseManager::getTestLicense));
        assertThat(licenseManager.getOwner().orElse("unknown")).isEqualTo("testing");
        assertThat(licenseManager.getType()).isEqualTo(LicenseType.UNKNOWN);
    }

    @Test
    public void testRaisesStarburstLicenseExceptionWhenNoProviderReturnsALicense()
    {
        LicenseManager licenseManager = new StarburstLicenseManager(PAST, ImmutableSet.of(Optional::empty));
        assertThatExceptionOfType(StarburstLicenseException.class)
                .isThrownBy(() -> licenseManager.checkFeature(A_FEATURE))
                .withMessage("Valid license required to use the feature: Warp Speed");
    }

    @Test
    public void testFeatureIsDisabledAfterExpiry()
    {
        LicenseManager licenseManager = new StarburstLicenseManager(FUTURE, ImmutableSet.of(TestStarburstLicenseManager::getTestLicense));
        assertThatExceptionOfType(StarburstLicenseException.class)
                .isThrownBy(() -> licenseManager.checkFeature(A_FEATURE))
                .withMessage("The license expired on: 2000-01-01T00:00, current local time is: %s", LocalDateTime.now(FUTURE));
    }

    @Test
    public void testNullFeatureRaisesNullPointerException()
    {
        LicenseManager licenseManager = new StarburstLicenseManager(PAST, ImmutableSet.of(TestStarburstLicenseManager::getTestLicense));
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(() -> licenseManager.checkFeature(null))
                .withMessage("feature is null");
    }
}
