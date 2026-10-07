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

import io.starburst.materialization.HashUtil;

import static io.starburst.materialization.HashUtil.combineHash;

/**
 * Index bucket key of a computation: its leaf table and the kinds of operators above it, never their keys, functions
 * or expressions. Folding those in would split MVs that differ only in what a match may ignore, such as extra
 * aggregations.
 */
public record ComputationHash(long hash)
{
    private static final long AGGREGATE = HashUtil.hash("Aggregation");

    public ComputationHash aggregateOf()
    {
        return new ComputationHash(combineHash(AGGREGATE, hash));
    }
}
