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

import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public enum StarburstFeature
{
    AGENTIC_LAYER("agentic-layer", "Agentic layer"),
    AI_WORKFLOWS("ai-workflows", "AI workflows"),
    DYNAMODB("dynamodb", "Starburst DynamoDB connector"),
    MCP("mcp", "MCP server"),
    SALESFORCE("salesforce", "Starburst Salesforce connector"),
    SPARK("spark", "Spark"),
    SPLUNK("splunk", "Starburst Splunk connector"),
    WARP_SPEED("warp-speed", "Warp Speed"),
    DELL("dell", "Dell Data Lakehouse", ImmutableSet.of(AGENTIC_LAYER, AI_WORKFLOWS, MCP, SPARK, WARP_SPEED)),
    /**/;

    private final String featureName;
    private final String displayName;
    private final Set<StarburstFeature> includedFeatures;

    StarburstFeature(String featureName, String displayName)
    {
        this(featureName, displayName, ImmutableSet.of());
    }

    StarburstFeature(String featureName, String displayName, Set<StarburstFeature> includedFeatures)
    {
        this.featureName = requireNonNull(featureName, "featureName is null");
        this.displayName = requireNonNull(displayName, "displayName is null");
        this.includedFeatures = ImmutableSet.copyOf(requireNonNull(includedFeatures, "includedFeatures is null"));
    }

    public String getFeatureName()
    {
        return featureName;
    }

    public String getDisplayName()
    {
        return displayName;
    }

    public Set<StarburstFeature> effectiveFeatures()
    {
        return ImmutableSet.<StarburstFeature>builder()
                .add(this)
                .addAll(includedFeatures.stream().flatMap(feature -> feature.effectiveFeatures().stream()).collect(toImmutableSet()))
                .build();
    }
}
