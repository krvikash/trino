/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.castleinsights;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Key;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCastleInsightsModule
{
    @Test
    public void testDisabledByDefault()
    {
        try (StandaloneQueryRunner queryRunner = new StandaloneQueryRunner(TEST_SESSION)) {
            assertThat(config(queryRunner).isEnabled()).isFalse();
        }
    }

    @Test
    public void testEnabled()
    {
        try (StandaloneQueryRunner queryRunner = new StandaloneQueryRunner(
                TEST_SESSION,
                builder -> builder.setProperties(ImmutableMap.of("castle-insights.enabled", "true")))) {
            assertThat(config(queryRunner).isEnabled()).isTrue();
        }
    }

    private static CastleInsightsConfig config(StandaloneQueryRunner queryRunner)
    {
        return queryRunner.getCoordinator().getInstance(Key.get(CastleInsightsConfig.class));
    }
}
