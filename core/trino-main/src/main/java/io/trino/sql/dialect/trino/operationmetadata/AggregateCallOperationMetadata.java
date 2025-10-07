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
import io.trino.metadata.ResolvedFunction;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.SortOrderList;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.TrinoAttributeSignature;
import io.trino.sql.planner.plan.AggregationNode;

import java.util.Set;

import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalBooleanAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalEnumAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalResolvedFunctionAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalSortOrderListAttributeMetadata;

public class AggregateCallOperationMetadata
        implements TrinoOperationMetadata
{
    public static final String NAME = "aggregate_call";

    private static final TrinoAttributeMetadata<SortOrderList> SORT_ORDERS_ATTRIBUTE_METADATA = internalSortOrderListAttributeMetadata(NAME, "sort_orders");
    private static final TrinoAttributeMetadata<ResolvedFunction> RESOLVED_FUNCTION_ATTRIBUTE_METADATA = internalResolvedFunctionAttributeMetadata(NAME, "resolved_function");
    private static final TrinoAttributeMetadata<Boolean> DISTINCT_ATTRIBUTE_METADATA = internalBooleanAttributeMetadata(NAME, "distinct");
    private static final TrinoAttributeMetadata<AggregationStep> AGGREGATION_STEP_ATTRIBUTE_METADATA = internalEnumAttributeMetadata(NAME, "aggregation_step", AggregationStep.class);

    public static final TrinoAttributeSignature<SortOrderList> SORT_ORDERS = SORT_ORDERS_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<ResolvedFunction> RESOLVED_FUNCTION = RESOLVED_FUNCTION_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<Boolean> DISTINCT = DISTINCT_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<AggregationStep> AGGREGATION_STEP = AGGREGATION_STEP_ATTRIBUTE_METADATA.trinoAttributeSignature();

    @Override
    public Set<TrinoAttributeMetadata<?>> operationAttributes()
    {
        return ImmutableSet.of(
                SORT_ORDERS_ATTRIBUTE_METADATA,
                RESOLVED_FUNCTION_ATTRIBUTE_METADATA,
                DISTINCT_ATTRIBUTE_METADATA,
                AGGREGATION_STEP_ATTRIBUTE_METADATA);
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
