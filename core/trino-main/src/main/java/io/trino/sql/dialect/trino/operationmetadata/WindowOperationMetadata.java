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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.dialect.trino.operationmetadata.AttributeDerivationUtils.defaultDeriveIrLevelAttributes;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalIntegerAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalIntegerListAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalSortOrderListAttributeMetadata;

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
