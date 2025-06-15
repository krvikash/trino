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
import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class TestStarburstFeature
{
    @Test
    public void testFeatureName()
    {
        for (StarburstFeature feature : StarburstFeature.values()) {
            assertThat(feature.getFeatureName()).as(feature.name() + ".featureName")
                    .isEqualTo(CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_HYPHEN, feature.name()));
        }
    }

    @Test
    public void testFeatureListSorted()
    {
        assertThat(Stream.of(StarburstFeature.values()))
                .map(StarburstFeature::name)
                .isSorted();
    }
}
