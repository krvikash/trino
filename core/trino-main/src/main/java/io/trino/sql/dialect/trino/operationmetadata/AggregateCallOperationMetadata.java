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
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.operation.AggregateCall;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.SortOrderList;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.TrinoAttributeSignature;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Operation.AttributeKey;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;
import io.trino.sql.planner.plan.AggregationNode;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.dialect.trino.operationmetadata.AttributeDerivationUtils.defaultDeriveFunctionCallIrLevelAttributes;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalBooleanAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalEnumAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalResolvedFunctionAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalSortOrderListAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.prefixedName;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.partitioningBy;

public class AggregateCallOperationMetadata
        implements TrinoOperationMetadata
{
    public static final String NAME = "aggregate_call";

    private static final TrinoAttributeMetadata<SortOrderList> SORT_ORDERS_ATTRIBUTE_METADATA = internalSortOrderListAttributeMetadata(NAME, "sort_orders");
    private static final TrinoAttributeMetadata<ResolvedFunction> RESOLVED_FUNCTION_ATTRIBUTE_METADATA = internalResolvedFunctionAttributeMetadata(NAME, "resolved_function");
    private static final TrinoAttributeMetadata<Boolean> DISTINCT_ATTRIBUTE_METADATA = internalBooleanAttributeMetadata(NAME, "distinct");
    private static final TrinoAttributeMetadata<AggregationStep> AGGREGATION_STEP_ATTRIBUTE_METADATA = internalEnumAttributeMetadata(NAME, "step", AggregationStep.class);

    public static final TrinoAttributeSignature<SortOrderList> SORT_ORDERS = SORT_ORDERS_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<ResolvedFunction> RESOLVED_FUNCTION = RESOLVED_FUNCTION_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<Boolean> DISTINCT = DISTINCT_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<AggregationStep> AGGREGATION_STEP = AGGREGATION_STEP_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<Type> RESULT_TYPE = new TrinoAttributeSignature<>(prefixedName(NAME, "result_type"), false);

    public static final Set<TrinoAttributeSignature<?>> OPERATION_ATTRIBUTES = ImmutableSet.of(SORT_ORDERS, RESOLVED_FUNCTION, DISTINCT, AGGREGATION_STEP, RESULT_TYPE);

    private final TrinoAttributeMetadata<Type> resultTypeTrinoAttributeMetadata;

    public AggregateCallOperationMetadata(Function<String, Type> typeDeserializer)
    {
        requireNonNull(typeDeserializer, "typeDeserializer is null");

        this.resultTypeTrinoAttributeMetadata = new TrinoAttributeMetadata<>(RESULT_TYPE, typeDeserializer, type -> type.getTypeId().getId());
    }

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
                RESOLVED_FUNCTION_ATTRIBUTE_METADATA,
                DISTINCT_ATTRIBUTE_METADATA,
                AGGREGATION_STEP_ATTRIBUTE_METADATA,
                resultTypeTrinoAttributeMetadata);
    }

    @Override
    public Operation createOperation(String resultName, List<Value> arguments, List<Region> regions, Map<AttributeKey, Object> attributes)
    {
        checkArgument(arguments.size() == 1, "AggregateCall operation must have exactly one argument: the input group");
        checkArgument(regions.size() == 4, "AggregateCall operation must have exactly four regions");

        Map<Boolean, List<Map.Entry<AttributeKey, Object>>> partitionedAttributes = attributes.entrySet().stream()
                .collect(partitioningBy(entry -> operationAttributeKeys().contains(entry.getKey())));
        Map<AttributeKey, Object> operationAttributes = ImmutableMap.copyOf(partitionedAttributes.get(true));
        Map<AttributeKey, Object> derivedAttributes = ImmutableMap.copyOf(partitionedAttributes.get(false));

        return new AggregateCall(
                resultName,
                getOnlyElement(arguments),
                RESULT_TYPE.getAttribute(operationAttributes),
                regions.get(0).getOnlyBlock().withLabel("^arguments"),
                regions.get(1).getOnlyBlock().withLabel("^filterSelector"),
                regions.get(2).getOnlyBlock().withLabel("^maskSelector"),
                regions.get(3).getOnlyBlock().withLabel("^orderingSelector"),
                Optional.ofNullable(SORT_ORDERS.getAttribute(operationAttributes)),
                RESOLVED_FUNCTION.getAttribute(operationAttributes),
                DISTINCT.getAttribute(operationAttributes),
                AGGREGATION_STEP.getAttribute(operationAttributes),
                derivedAttributes);
    }

    @Override
    public BiFunction<Map<AttributeKey, Object>, List<Map<AttributeKey, Object>>, Map<AttributeKey, Object>> attributeDerivation()
    {
        return AggregateCallOperationMetadata::deriveAttributes;
    }

    public static Map<AttributeKey, Object> deriveAttributes(Map<AttributeKey, Object> currentAttributes, List<Map<AttributeKey, Object>> childAttributes)
    {
        checkArgument(childAttributes.size() == 5, "AggregateCall operation must have exactly five child attributes maps: one for the input, and one for each of the four regions");

        ResolvedFunction resolvedFunction = RESOLVED_FUNCTION.getAttribute(currentAttributes);
        return defaultDeriveFunctionCallIrLevelAttributes(resolvedFunction, childAttributes);
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
