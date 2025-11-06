/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting;

import io.trino.tests.tpch.TpchQueryRunner;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatNoException;

public class TestQueryTroubleshootingWithNoUi
{
    @Test
    public void createQueryRunnerWorksWithWebUiDisabled()
    {
        assertThatNoException().isThrownBy(() -> TpchQueryRunner.builder()
                .setCoordinatorProperties(Map.of("web-ui.enabled", "false"))
                .build());
    }
}
