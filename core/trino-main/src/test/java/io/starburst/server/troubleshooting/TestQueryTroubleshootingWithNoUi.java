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

import com.starburstdata.presto.server.StarburstQueryRunner;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestQueryTroubleshootingWithNoUi
{
    @Test
    public void createQueryRunnerWorksWithWebUiDisabled()
    {
        assertThatNoException().isThrownBy(() -> StarburstQueryRunner.builder(testSessionBuilder().build())
                .setCoordinatorProperties(Map.of(
                        "web-ui.enabled", "false",
                        "web-ui.authentication.type", "insecure"))
                .build());
    }

    @Test
    public void troubleshootingIsNotAvailableWithWebUiDisabled()
    {
        assertThatThrownBy(() -> StarburstQueryRunner.builder(testSessionBuilder().build())
                .setCoordinatorProperties(Map.of(
                        "web-ui.enabled", "false",
                        "web-ui.authentication.type", "insecure",
                        // include a troubleshooting property to make sure it's not used:
                        "troubleshooting.max-access-duration", "20s"))
                .build())
                .hasMessageContaining("Configuration property 'troubleshooting.max-access-duration' was not used");
    }
}
