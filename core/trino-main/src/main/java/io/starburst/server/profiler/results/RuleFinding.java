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

import io.trino.spi.profiler.Severity;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public sealed interface RuleFinding
        permits RuleFinding.Node,
                RuleFinding.Query,
                RuleFinding.Stage
{
    Finding finding();

    record Node(String planNodeId, Finding finding)
            implements RuleFinding
    {
        public Node
        {
            requireNonNull(planNodeId, "planNodeId is null");
            requireNonNull(finding, "finding is null");
        }

        public Node(String planNodeId, String description)
        {
            this(planNodeId, new Finding(description));
        }

        public Node(String planNodeId, String description, Severity severity, Optional<String> recommendation)
        {
            this(planNodeId, new Finding(description, severity, recommendation));
        }
    }

    record Stage(String stageId, Finding finding)
            implements RuleFinding
    {
        public Stage
        {
            requireNonNull(stageId, "stageId is null");
            requireNonNull(finding, "finding is null");
        }

        public Stage(String stageId, String description)
        {
            this(stageId, new Finding(description));
        }

        public Stage(String stageId, String description, Severity severity, Optional<String> recommendation)
        {
            this(stageId, new Finding(description, severity, recommendation));
        }
    }

    record Query(Finding finding)
            implements RuleFinding
    {
        public Query
        {
            requireNonNull(finding, "finding is null");
        }

        public Query(String description)
        {
            this(new Finding(description));
        }

        public Query(String description, Severity severity, Optional<String> recommendation)
        {
            this(new Finding(description, severity, recommendation));
        }
    }
}
