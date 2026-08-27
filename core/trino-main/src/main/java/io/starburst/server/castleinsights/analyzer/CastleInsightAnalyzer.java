/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.castleinsights.analyzer;

import io.starburst.server.castleinsights.CastleQueryAnalysis;
import io.starburst.server.castleinsights.model.CastleInsight;

import java.util.List;

public interface CastleInsightAnalyzer
{
    List<CastleInsight> analyze(CastleQueryAnalysis analysis);
}
