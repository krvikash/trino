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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.metadata.ResolvedFunction;
import io.trino.sql.dialect.trino.operation.WindowFunctionCall;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.SortOrderList;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.TrinoAttributeSignature;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Operation.AttributeKey;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;
import io.trino.sql.planner.plan.FrameBoundType;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.dialect.trino.operationmetadata.AttributeDerivationUtils.defaultDeriveFunctionCallIrLevelAttributes;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalBooleanAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalEnumAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalResolvedFunctionAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalSortOrderListAttributeMetadata;
import static java.util.stream.Collectors.partitioningBy;

public class WindowFunctionCallOperationMetadata
        implements TrinoOperationMetadata
{
    public static final String NAME = "window_function_call";

    private static final TrinoAttributeMetadata<ResolvedFunction> RESOLVED_FUNCTION_ATTRIBUTE_METADATA = internalResolvedFunctionAttributeMetadata(NAME, "resolved_function");
    private static final TrinoAttributeMetadata<SortOrderList> SORT_ORDERS_ATTRIBUTE_METADATA = internalSortOrderListAttributeMetadata(NAME, "sort_orders");
    private static final TrinoAttributeMetadata<WindowFrameType> FRAME_TYPE_ATTRIBUTE_METADATA = internalEnumAttributeMetadata(NAME, "frame_type", WindowFrameType.class);
    private static final TrinoAttributeMetadata<WindowFrameBoundType> FRAME_START_TYPE_ATTRIBUTE_METADATA = internalEnumAttributeMetadata(NAME, "frame_start_type", WindowFrameBoundType.class);
    private static final TrinoAttributeMetadata<WindowFrameBoundType> FRAME_END_TYPE_ATTRIBUTE_METADATA = internalEnumAttributeMetadata(NAME, "frame_end_type", WindowFrameBoundType.class);
    private static final TrinoAttributeMetadata<Boolean> IGNORE_NULLS_ATTRIBUTE_METADATA = internalBooleanAttributeMetadata(NAME, "ignore_nulls");
    private static final TrinoAttributeMetadata<Boolean> DISTINCT_ATTRIBUTE_METADATA = internalBooleanAttributeMetadata(NAME, "distinct");

    public static final TrinoAttributeSignature<ResolvedFunction> RESOLVED_FUNCTION = RESOLVED_FUNCTION_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<SortOrderList> SORT_ORDERS = SORT_ORDERS_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<WindowFrameType> FRAME_TYPE = FRAME_TYPE_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<WindowFrameBoundType> FRAME_START_TYPE = FRAME_START_TYPE_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<WindowFrameBoundType> FRAME_END_TYPE = FRAME_END_TYPE_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<Boolean> IGNORE_NULLS = IGNORE_NULLS_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<Boolean> DISTINCT = DISTINCT_ATTRIBUTE_METADATA.trinoAttributeSignature();

    public static final Set<TrinoAttributeSignature<?>> OPERATION_ATTRIBUTES = ImmutableSet.of(
            RESOLVED_FUNCTION,
            SORT_ORDERS,
            FRAME_TYPE,
            FRAME_START_TYPE,
            FRAME_END_TYPE,
            IGNORE_NULLS,
            DISTINCT);

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public Set<TrinoAttributeMetadata<?>> operationAttributes()
    {
        return ImmutableSet.of(
                RESOLVED_FUNCTION_ATTRIBUTE_METADATA,
                SORT_ORDERS_ATTRIBUTE_METADATA,
                FRAME_TYPE_ATTRIBUTE_METADATA,
                FRAME_START_TYPE_ATTRIBUTE_METADATA,
                FRAME_END_TYPE_ATTRIBUTE_METADATA,
                IGNORE_NULLS_ATTRIBUTE_METADATA,
                DISTINCT_ATTRIBUTE_METADATA);
    }

    @Override
    public Operation createOperation(String resultName, List<Value> arguments, List<Region> regions, Map<AttributeKey, Object> attributes)
    {
        checkArgument(arguments.size() == 1, "WindowFunctionCall operation must have exactly one argument: the window");
        checkArgument(regions.size() == 6, "WindowFunctionCall operation must have exactly six regions");

        Map<Boolean, List<Map.Entry<AttributeKey, Object>>> partitionedAttributes = attributes.entrySet().stream()
                .collect(partitioningBy(entry -> inherentOperationAttributeKeys().contains(entry.getKey())));
        Map<AttributeKey, Object> operationAttributes = ImmutableMap.copyOf(partitionedAttributes.get(true));
        Map<AttributeKey, Object> derivedAttributes = ImmutableMap.copyOf(partitionedAttributes.get(false));

        return new WindowFunctionCall(
                resultName,
                getOnlyElement(arguments),
                regions.get(0).getOnlyBlock().withLabel("^arguments"),
                regions.get(1).getOnlyBlock().withLabel("^orderingSelector"),
                regions.get(2).getOnlyBlock().withLabel("^frameStartFieldSelector"),
                regions.get(3).getOnlyBlock().withLabel("^sortKeyCoercedForFrameStartComparisonSelector"),
                regions.get(4).getOnlyBlock().withLabel("^frameEndFieldSelector"),
                regions.get(5).getOnlyBlock().withLabel("^sortKeyCoercedForFrameEndComparisonSelector"),
                RESOLVED_FUNCTION.getAttribute(operationAttributes),
                Optional.ofNullable(SORT_ORDERS.getAttribute(operationAttributes)),
                FRAME_TYPE.getAttribute(operationAttributes),
                FRAME_START_TYPE.getAttribute(operationAttributes),
                FRAME_END_TYPE.getAttribute(operationAttributes),
                IGNORE_NULLS.getAttribute(operationAttributes),
                DISTINCT.getAttribute(operationAttributes),
                derivedAttributes);
    }

    @Override
    public BiFunction<Map<AttributeKey, Object>, List<Map<AttributeKey, Object>>, Map<AttributeKey, Object>> attributeDerivation()
    {
        return WindowFunctionCallOperationMetadata::deriveAttributes;
    }

    public static Map<AttributeKey, Object> deriveAttributes(Map<AttributeKey, Object> currentAttributes, List<Map<AttributeKey, Object>> childAttributes)
    {
        checkArgument(childAttributes.size() == 7, "WindowFunctionCall operation must have exactly seven child attributes maps: one for the input, and one for each of the six regions");

        ResolvedFunction resolvedFunction = RESOLVED_FUNCTION.getAttribute(currentAttributes);
        return defaultDeriveFunctionCallIrLevelAttributes(resolvedFunction, childAttributes);
    }

    public enum WindowFrameType
    {
        RANGE,
        ROWS,
        GROUPS;

        public static WindowFrameType of(io.trino.sql.planner.plan.WindowFrameType type)
        {
            return switch (type) {
                case RANGE -> RANGE;
                case ROWS -> ROWS;
                case GROUPS -> GROUPS;
            };
        }
    }

    public enum WindowFrameBoundType
    {
        UNBOUNDED_PRECEDING,
        PRECEDING,
        CURRENT_ROW,
        FOLLOWING,
        UNBOUNDED_FOLLOWING;

        public static WindowFrameBoundType of(FrameBoundType type)
        {
            return switch (type) {
                case UNBOUNDED_PRECEDING -> UNBOUNDED_PRECEDING;
                case PRECEDING -> PRECEDING;
                case CURRENT_ROW -> CURRENT_ROW;
                case FOLLOWING -> FOLLOWING;
                case UNBOUNDED_FOLLOWING -> UNBOUNDED_FOLLOWING;
            };
        }
    }
}
