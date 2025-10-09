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

import java.util.Set;

import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalBooleanAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalSortOrderListAttributeMetadata;

public class SortOperationMetadata
        implements TrinoOperationMetadata
{
    public static final String NAME = "sort";

    private static final TrinoAttributeMetadata<SortOrderList> SORT_ORDERS_ATTRIBUTE_METADATA = internalSortOrderListAttributeMetadata(NAME, "sort_orders");
    private static final TrinoAttributeMetadata<Boolean> PARTIAL_ATTRIBUTE_METADATA = internalBooleanAttributeMetadata(NAME, "partial");

    public static final TrinoAttributeSignature<SortOrderList> SORT_ORDERS = SORT_ORDERS_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<Boolean> PARTIAL = PARTIAL_ATTRIBUTE_METADATA.trinoAttributeSignature();

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
                PARTIAL_ATTRIBUTE_METADATA);
    }
}
