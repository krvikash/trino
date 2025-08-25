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

import com.google.common.base.CaseFormat;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class TestStarburstFeature
{
    @ParameterizedTest
    @EnumSource(StarburstFeature.class)
    public void testFeatureName(StarburstFeature feature)
    {
        assertThat(feature.getFeatureName()).as(feature.name() + ".featureName")
                .isEqualTo(CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_HYPHEN, feature.name()));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testFeatureListSorted(boolean compositeFeature)
    {
        assertThat(Stream.of(StarburstFeature.values()))
                // we can't enforce global ordering because composite features must appear after non-composite (they can
                // only reference features already defined); we check ordering separately for composite and non-composite
                .filteredOn(feature -> compositeFeature ^ (feature.effectiveFeatures().size() == 1))
                .map(StarburstFeature::name)
                .isSorted();
    }
}
