/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.sql.dialect.trino.operationmetadata;

import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodecFactory;
import io.trino.cost.PlanNodeStatsAndCostSummary;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.TrinoAttributeSignature;
import io.trino.sql.planner.plan.JoinNode;

import java.util.List;
import java.util.Set;

import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalBooleanAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalEnumAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalObjectAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalStringListAttributeMetadata;

public class JoinOperationMetadata
        implements TrinoOperationMetadata
{
    public static final String NAME = "join";

    private static final TrinoAttributeMetadata<JoinType> JOIN_TYPE_ATTRIBUTE_METADATA = internalEnumAttributeMetadata(NAME, "join_type", JoinType.class);
    private static final TrinoAttributeMetadata<Boolean> MAY_SKIP_OUTPUT_DUPLICATES_ATTRIBUTE_METADATA = internalBooleanAttributeMetadata(NAME, "may_skip_output_duplicates");
    private static final TrinoAttributeMetadata<DistributionType> DISTRIBUTION_TYPE_ATTRIBUTE_METADATA = internalEnumAttributeMetadata(NAME, "distribution_type", DistributionType.class);
    private static final TrinoAttributeMetadata<Boolean> SPILLABLE_ATTRIBUTE_METADATA = internalBooleanAttributeMetadata(NAME, "spillable");
    private static final TrinoAttributeMetadata<List<String>> DYNAMIC_FILTER_IDS_ATTRIBUTE_METADATA = internalStringListAttributeMetadata(NAME, "dynamic_filter_ids");
    private static final TrinoAttributeMetadata<PlanNodeStatsAndCostSummary> STATISTICS_AND_COST_SUMMARY_ATTRIBUTE_METADATA = internalObjectAttributeMetadata(NAME, "statistics_and_cost_summary", new JsonCodecFactory().jsonCodec(PlanNodeStatsAndCostSummary.class));

    public static final TrinoAttributeSignature<JoinType> JOIN_TYPE = JOIN_TYPE_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<Boolean> MAY_SKIP_OUTPUT_DUPLICATES = MAY_SKIP_OUTPUT_DUPLICATES_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<DistributionType> DISTRIBUTION_TYPE = DISTRIBUTION_TYPE_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<Boolean> SPILLABLE = SPILLABLE_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<List<String>> DYNAMIC_FILTER_IDS = DYNAMIC_FILTER_IDS_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<PlanNodeStatsAndCostSummary> STATISTICS_AND_COST_SUMMARY = STATISTICS_AND_COST_SUMMARY_ATTRIBUTE_METADATA.trinoAttributeSignature();

    @Override
    public Set<TrinoAttributeMetadata<?>> operationAttributes()
    {
        return ImmutableSet.of(
                JOIN_TYPE_ATTRIBUTE_METADATA,
                MAY_SKIP_OUTPUT_DUPLICATES_ATTRIBUTE_METADATA,
                DISTRIBUTION_TYPE_ATTRIBUTE_METADATA,
                SPILLABLE_ATTRIBUTE_METADATA,
                DYNAMIC_FILTER_IDS_ATTRIBUTE_METADATA,
                STATISTICS_AND_COST_SUMMARY_ATTRIBUTE_METADATA);
    }

    public enum JoinType
    {
        INNER,
        LEFT,
        RIGHT,
        FULL;

        public static JoinType of(io.trino.sql.planner.plan.JoinType joinType)
        {
            return switch (joinType) {
                case INNER -> INNER;
                case LEFT -> LEFT;
                case RIGHT -> RIGHT;
                case FULL -> FULL;
            };
        }
    }

    public enum DistributionType
    {
        PARTITIONED,
        REPLICATED;

        public static DistributionType of(JoinNode.DistributionType distributionType)
        {
            return switch (distributionType) {
                case PARTITIONED -> PARTITIONED;
                case REPLICATED -> REPLICATED;
            };
        }
    }
}
