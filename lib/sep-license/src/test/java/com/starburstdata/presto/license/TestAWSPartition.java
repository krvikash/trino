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

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TestAWSPartition
{
    @Test
    public void testAWSPartition()
    {
        Map<String, AWSPartition> expectedRegions = ImmutableMap.<String, AWSPartition>builder()
                .put("us-east-2", AWSPartition.AWS)
                .put("us-east-1", AWSPartition.AWS)
                .put("us-west-1", AWSPartition.AWS)
                .put("us-west-2", AWSPartition.AWS)
                .put("af-south-1", AWSPartition.AWS)
                .put("ap-east-1", AWSPartition.AWS)
                .put("ap-south-1", AWSPartition.AWS)
                .put("ap-northeast-3", AWSPartition.AWS)
                .put("ap-northeast-2", AWSPartition.AWS)
                .put("ap-southeast-1", AWSPartition.AWS)
                .put("ap-southeast-2", AWSPartition.AWS)
                .put("ap-northeast-1", AWSPartition.AWS)
                .put("ca-central-1", AWSPartition.AWS)
                .put("cn-north-1", AWSPartition.AWS_CN)
                .put("cn-northwest-1", AWSPartition.AWS_CN)
                .put("eu-central-1", AWSPartition.AWS)
                .put("eu-west-1", AWSPartition.AWS)
                .put("eu-west-2", AWSPartition.AWS)
                .put("eu-south-1", AWSPartition.AWS)
                .put("eu-west-3", AWSPartition.AWS)
                .put("eu-north-1", AWSPartition.AWS)
                .put("me-south-1", AWSPartition.AWS)
                .put("sa-east-1", AWSPartition.AWS)
                .put("us-gov-east-1", AWSPartition.AWS_US_GOV)
                .put("us-gov-west-1", AWSPartition.AWS_US_GOV)
                .buildOrThrow();
        for (Map.Entry<String, AWSPartition> entry : expectedRegions.entrySet()) {
            AWSPartition actualPartition = AWSPartition.fromRegion(entry.getKey());
            assertThat(actualPartition).as("Region %s in partition %s", entry.getKey(), actualPartition.name()).isEqualTo(entry.getValue());
        }
    }
}
