/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.profiler.results;

import static java.util.Objects.requireNonNull;

public sealed interface RuleFinding
        permits RuleFinding.Node,
                RuleFinding.Query,
                RuleFinding.Stage
{
    String description();

    record Node(String planNodeId, String description)
            implements RuleFinding
    {
        public Node
        {
            requireNonNull(planNodeId, "planNodeId is null");
            requireNonNull(description, "description is null");
        }
    }

    record Stage(String stageId, String description)
            implements RuleFinding
    {
        public Stage
        {
            requireNonNull(stageId, "stageId is null");
            requireNonNull(description, "description is null");
        }
    }

    record Query(String description)
            implements RuleFinding
    {
        public Query
        {
            requireNonNull(description, "description is null");
        }
    }
}
