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

import static java.util.Objects.requireNonNull;

public enum StarburstFeature
{
    AI_WORKFLOWS("ai-workflows", "AI workflows"),
    DYNAMODB("dynamodb", "Starburst DynamoDB connector"),
    SALESFORCE("salesforce", "Starburst Salesforce connector"),
    SPARK("spark", "Spark"),
    SPLUNK("splunk", "Starburst Splunk connector"),
    WARP_SPEED("warp-speed", "Warp Speed"),
    /**/;

    private final String featureName;
    private final String displayName;

    StarburstFeature(String featureName, String displayName)
    {
        this.featureName = requireNonNull(featureName, "featureName is null");
        this.displayName = requireNonNull(displayName, "displayName is null");
    }

    public String getFeatureName()
    {
        return featureName;
    }

    public String getDisplayName()
    {
        return displayName;
    }
}
