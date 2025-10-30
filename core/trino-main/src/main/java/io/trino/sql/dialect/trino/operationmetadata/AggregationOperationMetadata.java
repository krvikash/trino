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
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.TrinoAttributeSignature;
import io.trino.sql.newir.Operation.AttributeKey;
import io.trino.sql.planner.plan.AggregationNode;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.dialect.trino.operationmetadata.AttributeDerivationUtils.defaultDeriveIrLevelAttributes;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalBooleanAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalEnumAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalIntegerAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalIntegerListAttributeMetadata;

public class AggregationOperationMetadata
        implements TrinoOperationMetadata
{
    public static final String NAME = "aggregation";

    private static final TrinoAttributeMetadata<Integer> GROUPING_SETS_COUNT_ATTRIBUTE_METADATA = internalIntegerAttributeMetadata(NAME, "grouping_sets_count");
    private static final TrinoAttributeMetadata<List<Integer>> GLOBAL_GROUPING_SETS_ATTRIBUTE_METADATA = internalIntegerListAttributeMetadata(NAME, "global_grouping_sets");
    private static final TrinoAttributeMetadata<Integer> GROUP_ID_INDEX_ATTRIBUTE_METADATA = internalIntegerAttributeMetadata(NAME, "group_id_index");
    private static final TrinoAttributeMetadata<List<Integer>> PRE_GROUPED_INDEXES_ATTRIBUTE_METADATA = internalIntegerListAttributeMetadata(NAME, "pre_grouped_indexes");
    private static final TrinoAttributeMetadata<AggregationStep> AGGREGATION_STEP_ATTRIBUTE_METADATA = internalEnumAttributeMetadata(NAME, "step", AggregationStep.class);
    private static final TrinoAttributeMetadata<Boolean> INPUT_REDUCING_ATTRIBUTE_METADATA = internalBooleanAttributeMetadata(NAME, "input_reducing");

    public static final TrinoAttributeSignature<Integer> GROUPING_SETS_COUNT = GROUPING_SETS_COUNT_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<List<Integer>> GLOBAL_GROUPING_SETS = GLOBAL_GROUPING_SETS_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<Integer> GROUP_ID_INDEX = GROUP_ID_INDEX_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<List<Integer>> PRE_GROUPED_INDEXES = PRE_GROUPED_INDEXES_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<AggregationStep> AGGREGATION_STEP = AGGREGATION_STEP_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<Boolean> INPUT_REDUCING = INPUT_REDUCING_ATTRIBUTE_METADATA.trinoAttributeSignature();

    public static final Set<TrinoAttributeSignature<?>> OPERATION_ATTRIBUTES = ImmutableSet.of(
            GROUPING_SETS_COUNT,
            GLOBAL_GROUPING_SETS,
            GROUP_ID_INDEX,
            PRE_GROUPED_INDEXES,
            AGGREGATION_STEP,
            INPUT_REDUCING);

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public Set<TrinoAttributeMetadata<?>> operationAttributes()
    {
        return ImmutableSet.of(
                GROUPING_SETS_COUNT_ATTRIBUTE_METADATA,
                GLOBAL_GROUPING_SETS_ATTRIBUTE_METADATA,
                GROUP_ID_INDEX_ATTRIBUTE_METADATA,
                PRE_GROUPED_INDEXES_ATTRIBUTE_METADATA,
                AGGREGATION_STEP_ATTRIBUTE_METADATA,
                INPUT_REDUCING_ATTRIBUTE_METADATA);
    }

    @Override
    public BiFunction<Map<AttributeKey, Object>, List<Map<AttributeKey, Object>>, Map<AttributeKey, Object>> attributeDerivation()
    {
        return AggregationOperationMetadata::deriveAttributes;
    }

    public static Map<AttributeKey, Object> deriveAttributes(Map<AttributeKey, Object> currentAttributes, List<Map<AttributeKey, Object>> childAttributes)
    {
        checkArgument(childAttributes.size() == 3, "Aggregation operation must have exactly three child attributes maps: one for the input, and one for each of the two regions");

        // TODO add more external attributes based on AggregationNode, for example: produces distinct rows, is decomposable,...
        return defaultDeriveIrLevelAttributes(childAttributes);
    }

    public enum AggregationStep
    {
        PARTIAL,
        FINAL,
        INTERMEDIATE,
        SINGLE;

        public static AggregationStep of(AggregationNode.Step step)
        {
            return switch (step) {
                case PARTIAL -> PARTIAL;
                case FINAL -> FINAL;
                case INTERMEDIATE -> INTERMEDIATE;
                case SINGLE -> SINGLE;
            };
        }
    }
}
