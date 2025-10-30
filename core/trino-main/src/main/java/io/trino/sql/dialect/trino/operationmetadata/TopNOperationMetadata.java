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
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.SortOrderList;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.TrinoAttributeSignature;
import io.trino.sql.newir.Operation.AttributeKey;
import io.trino.sql.planner.plan.TopNNode;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.dialect.trino.operationmetadata.AttributeDerivationUtils.defaultDeriveIrLevelAttributes;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalEnumAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalLongAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalSortOrderListAttributeMetadata;

public class TopNOperationMetadata
        implements TrinoOperationMetadata
{
    public static final String NAME = "top_n";

    private static final TrinoAttributeMetadata<SortOrderList> SORT_ORDERS_ATTRIBUTE_METADATA = internalSortOrderListAttributeMetadata(NAME, "sort_orders");
    private static final TrinoAttributeMetadata<Long> LIMIT_ATTRIBUTE_METADATA = internalLongAttributeMetadata(NAME, "limit");
    private static final TrinoAttributeMetadata<TopNStep> TOP_N_STEP_ATTRIBUTE_METADATA = internalEnumAttributeMetadata(NAME, "step", TopNStep.class);

    public static final TrinoAttributeSignature<SortOrderList> SORT_ORDERS = SORT_ORDERS_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<Long> LIMIT = LIMIT_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<TopNStep> TOP_N_STEP = TOP_N_STEP_ATTRIBUTE_METADATA.trinoAttributeSignature();

    public static final Set<TrinoAttributeSignature<?>> OPERATION_ATTRIBUTES = ImmutableSet.of(SORT_ORDERS, LIMIT, TOP_N_STEP);

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public Set<TrinoAttributeMetadata<?>> operationAttributes()
    {
        return ImmutableSet.of(
                SORT_ORDERS_ATTRIBUTE_METADATA,
                LIMIT_ATTRIBUTE_METADATA,
                TOP_N_STEP_ATTRIBUTE_METADATA);
    }

    @Override
    public BiFunction<Map<AttributeKey, Object>, List<Map<AttributeKey, Object>>, Map<AttributeKey, Object>> attributeDerivation()
    {
        return TopNOperationMetadata::deriveAttributes;
    }

    public static Map<AttributeKey, Object> deriveAttributes(Map<AttributeKey, Object> currentAttributes, List<Map<AttributeKey, Object>> childAttributes)
    {
        checkArgument(childAttributes.size() == 2, "TopN operation must have exactly two child attributes maps: one for the input, and one for the ordering selector");

        return defaultDeriveIrLevelAttributes(childAttributes);
    }

    public enum TopNStep
    {
        SINGLE,
        PARTIAL,
        FINAL;

        public static TopNStep of(TopNNode.Step step)
        {
            return switch (step) {
                case SINGLE -> SINGLE;
                case PARTIAL -> PARTIAL;
                case FINAL -> FINAL;
            };
        }
    }
}
