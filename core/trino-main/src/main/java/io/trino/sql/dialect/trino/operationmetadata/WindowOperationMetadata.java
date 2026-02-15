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
import io.trino.sql.dialect.trino.operation.Window;
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
import static io.trino.sql.dialect.trino.operationmetadata.AttributeDerivationUtils.defaultDeriveIrLevelAttributes;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalIntegerAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalIntegerListAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalSortOrderListAttributeMetadata;
import static java.util.stream.Collectors.partitioningBy;

public class WindowOperationMetadata
        implements TrinoOperationMetadata
{
    public static final String NAME = "window";

    private static final TrinoAttributeMetadata<List<Integer>> PRE_PARTITIONED_INDEXES_ATTRIBUTE_METADATA = internalIntegerListAttributeMetadata(NAME, "pre_partitioned_indexes");
    private static final TrinoAttributeMetadata<SortOrderList> SORT_ORDERS_ATTRIBUTE_METADATA = internalSortOrderListAttributeMetadata(NAME, "sort_orders");
    private static final TrinoAttributeMetadata<Integer> PRE_SORTED_PREFIX_ATTRIBUTE_METADATA = internalIntegerAttributeMetadata(NAME, "pre_sorted_prefix");

    public static final TrinoAttributeSignature<List<Integer>> PRE_PARTITIONED_INDEXES = PRE_PARTITIONED_INDEXES_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<SortOrderList> SORT_ORDERS = SORT_ORDERS_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<Integer> PRE_SORTED_PREFIX = PRE_SORTED_PREFIX_ATTRIBUTE_METADATA.trinoAttributeSignature();

    public static final Set<TrinoAttributeSignature<?>> OPERATION_ATTRIBUTES = ImmutableSet.of(PRE_PARTITIONED_INDEXES, SORT_ORDERS, PRE_SORTED_PREFIX);

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public Set<TrinoAttributeMetadata<?>> operationAttributes()
    {
        return ImmutableSet.of(
                PRE_PARTITIONED_INDEXES_ATTRIBUTE_METADATA,
                SORT_ORDERS_ATTRIBUTE_METADATA,
                PRE_SORTED_PREFIX_ATTRIBUTE_METADATA);
    }

    @Override
    public Operation createOperation(String resultName, List<Value> arguments, List<Region> regions, Map<AttributeKey, Object> attributes)
    {
        checkArgument(arguments.size() == 1, "Window operation must have exactly one argument: the input relation");
        checkArgument(regions.size() == 3, "Window operation must have exactly three regions");

        Map<Boolean, List<Map.Entry<AttributeKey, Object>>> partitionedAttributes = attributes.entrySet().stream()
                .collect(partitioningBy(entry -> inherentOperationAttributeKeys().contains(entry.getKey())));
        Map<AttributeKey, Object> operationAttributes = ImmutableMap.copyOf(partitionedAttributes.get(true));
        Map<AttributeKey, Object> derivedAttributes = ImmutableMap.copyOf(partitionedAttributes.get(false));

        return new Window(
                resultName,
                getOnlyElement(arguments),
                regions.get(0).getOnlyBlock().withLabel("^windowFunctions"),
                regions.get(1).getOnlyBlock().withLabel("^partitioningSelector"),
                regions.get(2).getOnlyBlock().withLabel("^orderingSelector"),
                PRE_PARTITIONED_INDEXES.getAttribute(operationAttributes),
                Optional.ofNullable(SORT_ORDERS.getAttribute(operationAttributes)),
                PRE_SORTED_PREFIX.getAttribute(operationAttributes),
                ImmutableMap.of(),
                derivedAttributes);
    }

    @Override
    public BiFunction<Map<AttributeKey, Object>, List<Map<AttributeKey, Object>>, Map<AttributeKey, Object>> attributeDerivation()
    {
        return WindowOperationMetadata::deriveAttributes;
    }

    public static Map<AttributeKey, Object> deriveAttributes(Map<AttributeKey, Object> currentAttributes, List<Map<AttributeKey, Object>> childAttributes)
    {
        checkArgument(childAttributes.size() == 4, "Window operation must have exactly four child attributes maps: one for the input, and one for each of the three regions");

        return defaultDeriveIrLevelAttributes(childAttributes);
    }
}
