/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.profiler.rules;

import io.starburst.server.profiler.QueryExecutionDetails;
import io.starburst.server.profiler.results.RuleFinding;

import java.util.List;

public interface QueryProfilerRule
{
    List<RuleFinding> analyze(QueryExecutionDetails details);
}
