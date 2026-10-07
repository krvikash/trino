/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.substitution;

import io.starburst.materialization.ir.Aggregation;
import io.trino.sql.planner.plan.AggregationNode;
import org.junit.jupiter.api.Test;

import static io.starburst.server.substitution.MaterializationIrExtractor.toIrStep;
import static org.assertj.core.api.Assertions.assertThat;

class TestMaterializationIrExtractor
{
    @Test
    void testEveryStepMapsToSameNamedIrStep()
    {
        // The matcher compares the persisted step against the query's through this mapping
        for (AggregationNode.Step step : AggregationNode.Step.values()) {
            assertThat(toIrStep(step).name()).isEqualTo(step.name());
        }
        assertThat(Aggregation.Step.values()).hasSameSizeAs(AggregationNode.Step.values());
    }
}
