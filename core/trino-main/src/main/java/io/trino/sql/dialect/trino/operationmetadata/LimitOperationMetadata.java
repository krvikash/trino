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
import io.trino.sql.dialect.ir.IrAttributeUtils;
import io.trino.sql.dialect.trino.operation.Limit;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.SortOrderList;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.TrinoAttributeSignature;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Operation.AttributeKey;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.dialect.ir.IrAttributeUtils.deterministic;
import static io.trino.sql.dialect.ir.IrAttributeUtils.hasNoSideEffects;
import static io.trino.sql.dialect.ir.IrAttributeUtils.hasSideEffects;
import static io.trino.sql.dialect.ir.IrAttributeUtils.isKnownDeterministic;
import static io.trino.sql.dialect.ir.IrAttributeUtils.nonIdempotent;
import static io.trino.sql.dialect.ir.IrAttributeUtils.safe;
import static io.trino.sql.dialect.ir.IrAttributeUtils.unsafe;
import static io.trino.sql.dialect.trino.operationmetadata.AttributeDerivationUtils.getRepeatabilityAttribute;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalBooleanAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalIntegerListAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalLongAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalSortOrderListAttributeMetadata;
import static java.util.stream.Collectors.partitioningBy;

public class LimitOperationMetadata
        implements TrinoOperationMetadata
{
    public static final String NAME = "limit";

    private static final TrinoAttributeMetadata<SortOrderList> SORT_ORDERS_ATTRIBUTE_METADATA = internalSortOrderListAttributeMetadata(NAME, "sort_orders");
    private static final TrinoAttributeMetadata<Long> COUNT_ATTRIBUTE_METADATA = internalLongAttributeMetadata(NAME, "count");
    private static final TrinoAttributeMetadata<Boolean> PARTIAL_ATTRIBUTE_METADATA = internalBooleanAttributeMetadata(NAME, "partial");
    private static final TrinoAttributeMetadata<List<Integer>> PRE_SORTED_INDEXES_ATTRIBUTE_METADATA = internalIntegerListAttributeMetadata(NAME, "pre_sorted_indexes");

    public static final TrinoAttributeSignature<SortOrderList> SORT_ORDERS = SORT_ORDERS_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<Long> COUNT = COUNT_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<Boolean> PARTIAL = PARTIAL_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<List<Integer>> PRE_SORTED_INDEXES = PRE_SORTED_INDEXES_ATTRIBUTE_METADATA.trinoAttributeSignature();

    public static final Set<TrinoAttributeSignature<?>> OPERATION_ATTRIBUTES = ImmutableSet.of(SORT_ORDERS, COUNT, PARTIAL, PRE_SORTED_INDEXES);

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
                COUNT_ATTRIBUTE_METADATA,
                PARTIAL_ATTRIBUTE_METADATA,
                PRE_SORTED_INDEXES_ATTRIBUTE_METADATA);
    }

    @Override
    public Operation createOperation(String resultName, List<Value> arguments, List<Region> regions, Map<AttributeKey, Object> attributes)
    {
        checkArgument(arguments.size() == 1, "Limit operation must have exactly one argument: the input relation");
        checkArgument(regions.size() == 1, "Limit operation must have exactly one region: the ordering selector");

        Map<Boolean, List<Map.Entry<AttributeKey, Object>>> partitionedAttributes = attributes.entrySet().stream()
                .collect(partitioningBy(entry -> operationAttributeKeys().contains(entry.getKey())));
        Map<AttributeKey, Object> operationAttributes = ImmutableMap.copyOf(partitionedAttributes.get(true));
        Map<AttributeKey, Object> derivedAttributes = ImmutableMap.copyOf(partitionedAttributes.get(false));

        return new Limit(
                resultName,
                getOnlyElement(arguments),
                getOnlyElement(regions).getOnlyBlock().withLabel("^orderingSelector"),
                Optional.ofNullable(SORT_ORDERS.getAttribute(operationAttributes)),
                COUNT.getAttribute(operationAttributes),
                PARTIAL.getAttribute(operationAttributes),
                PRE_SORTED_INDEXES.getAttribute(operationAttributes),
                ImmutableMap.of(),
                derivedAttributes);
    }

    @Override
    public BiFunction<Map<AttributeKey, Object>, List<Map<AttributeKey, Object>>, Map<AttributeKey, Object>> attributeDerivation()
    {
        return LimitOperationMetadata::deriveAttributes;
    }

    public static Map<AttributeKey, Object> deriveAttributes(Map<AttributeKey, Object> currentAttributes, List<Map<AttributeKey, Object>> childAttributes)
    {
        checkArgument(childAttributes.size() == 2, "Limit operation must have exactly two child attributes maps: one for the input, and one for the ordering selector");

        ImmutableMap.Builder<AttributeKey, Object> derivedAttributes = ImmutableMap.builder();

        boolean isWithTies = SORT_ORDERS.getAttribute(currentAttributes) != null;
        if (isWithTies) {
            if (childAttributes.stream().allMatch(IrAttributeUtils::isKnownDeterministic)) {
                deterministic(derivedAttributes);
            }
        }
        else {
            Map<AttributeKey, Object> inputAttributes = childAttributes.getFirst();
            // Limit operation without ties is non-idempotent in that it might return arbitrary subset of rows from its input
            if (isKnownDeterministic(inputAttributes)) {
                nonIdempotent(derivedAttributes);
            }
            else {
                derivedAttributes.putAll(getRepeatabilityAttribute(inputAttributes));
            }
        }

        if (childAttributes.stream().allMatch(IrAttributeUtils::isKnownSafe)) {
            safe(derivedAttributes);
        }
        else if (childAttributes.stream().anyMatch(IrAttributeUtils::isKnownUnsafe)) {
            unsafe(derivedAttributes);
        }

        if (childAttributes.stream().anyMatch(IrAttributeUtils::isKnownHasSideEffects)) {
            hasSideEffects(derivedAttributes);
        }
        else if (childAttributes.stream().allMatch(IrAttributeUtils::isKnownHasNoSideEffects)) {
            hasNoSideEffects(derivedAttributes);
        }

        return derivedAttributes.buildOrThrow();
    }
}
