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
import io.airlift.json.JsonCodec;
import io.trino.spi.predicate.NullableValue;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.SortOrderList;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.TrinoAttributeSignature;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.plan.ExchangeNode;
import org.assertj.core.util.VisibleForTesting;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalBooleanAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalEnumAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalIntegerAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalIntegerListAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalSortOrderListAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.prefixedName;
import static java.util.Objects.requireNonNull;

public class ExchangeOperationMetadata
        implements TrinoOperationMetadata
{
    public static final String NAME = "exchange";

    private static final TrinoAttributeMetadata<ExchangeType> EXCHANGE_TYPE_ATTRIBUTE_METADATA = internalEnumAttributeMetadata(NAME, "type", ExchangeType.class);
    private static final TrinoAttributeMetadata<ExchangeScope> EXCHANGE_SCOPE_ATTRIBUTE_METADATA = internalEnumAttributeMetadata(NAME, "scope", ExchangeScope.class);
    private static final TrinoAttributeMetadata<Boolean> REPLICATE_NULLS_AND_ANY_ATTRIBUTE_METADATA = internalBooleanAttributeMetadata(NAME, "replicate_nulls_and_any");
    private static final TrinoAttributeMetadata<List<Integer>> BUCKET_TO_PARTITION_ATTRIBUTE_METADATA = internalIntegerListAttributeMetadata(NAME, "bucket_to_partition");
    private static final TrinoAttributeMetadata<Integer> PARTITION_COUNT_ATTRIBUTE_METADATA = internalIntegerAttributeMetadata(NAME, "partition_count");
    private static final TrinoAttributeMetadata<Integer> BUCKET_COUNT_ATTRIBUTE_METADATA = internalIntegerAttributeMetadata(NAME, "bucket_count");
    private static final TrinoAttributeMetadata<SortOrderList> SORT_ORDERS_ATTRIBUTE_METADATA = internalSortOrderListAttributeMetadata(NAME, "sort_orders");

    public static final TrinoAttributeSignature<ExchangeType> EXCHANGE_TYPE = EXCHANGE_TYPE_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<ExchangeScope> EXCHANGE_SCOPE = EXCHANGE_SCOPE_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<Boolean> REPLICATE_NULLS_AND_ANY = REPLICATE_NULLS_AND_ANY_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<List<Integer>> BUCKET_TO_PARTITION = BUCKET_TO_PARTITION_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<Integer> PARTITION_COUNT = PARTITION_COUNT_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<Integer> BUCKET_COUNT = BUCKET_COUNT_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<SortOrderList> SORT_ORDERS = SORT_ORDERS_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<PartitioningHandle> PARTITIONING_HANDLE = new TrinoAttributeSignature<>(prefixedName(NAME, "partitioning_handle"), false);
    public static final TrinoAttributeSignature<NullableValues> NULLABLE_VALUES = new TrinoAttributeSignature<>(prefixedName(NAME, "nullable_values"), false);

    public static final Set<TrinoAttributeSignature<?>> OPERATION_ATTRIBUTES = ImmutableSet.of(
            EXCHANGE_TYPE,
            EXCHANGE_SCOPE,
            REPLICATE_NULLS_AND_ANY,
            BUCKET_TO_PARTITION,
            PARTITION_COUNT,
            BUCKET_COUNT,
            SORT_ORDERS,
            PARTITIONING_HANDLE,
            NULLABLE_VALUES);

    private final TrinoAttributeMetadata<PartitioningHandle> partitioningHandleTrinoAttributeMetadata;
    private final TrinoAttributeMetadata<NullableValues> nullableValuesTrinoAttributeMetadata;

    public ExchangeOperationMetadata(JsonCodec<PartitioningHandle> partitioningHandleCodec, JsonCodec<NullableValue[]> nullableValueArrayCodec)
    {
        this(
                partitioningHandleCodec::fromJson,
                partitioningHandleCodec::toJson,
                string -> new NullableValues(nullableValueArrayCodec.fromJson(string)),
                nullableValues -> nullableValueArrayCodec.toJson(nullableValues.nullableValues()));
    }

    @VisibleForTesting
    public ExchangeOperationMetadata(
            Function<String, PartitioningHandle> partitioningHandleParseMethod,
            Function<PartitioningHandle, String> partitioningHandlePrintMethod,
            Function<String, NullableValues> nullableValuesParseMethod,
            Function<NullableValues, String> nullableValuesPrintMethod)
    {
        requireNonNull(partitioningHandleParseMethod, "partitioningHandleParseMethod is null");
        requireNonNull(partitioningHandlePrintMethod, "partitioningHandlePrintMethod is null");
        requireNonNull(nullableValuesParseMethod, "nullableValuesParseMethod is null");
        requireNonNull(nullableValuesPrintMethod, "nullableValuesPrintMethod is null");

        this.partitioningHandleTrinoAttributeMetadata = new TrinoAttributeMetadata<>(PARTITIONING_HANDLE, partitioningHandleParseMethod, partitioningHandlePrintMethod);
        this.nullableValuesTrinoAttributeMetadata = new TrinoAttributeMetadata<>(NULLABLE_VALUES, nullableValuesParseMethod, nullableValuesPrintMethod);
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
                EXCHANGE_TYPE_ATTRIBUTE_METADATA,
                EXCHANGE_SCOPE_ATTRIBUTE_METADATA,
                REPLICATE_NULLS_AND_ANY_ATTRIBUTE_METADATA,
                BUCKET_TO_PARTITION_ATTRIBUTE_METADATA,
                PARTITION_COUNT_ATTRIBUTE_METADATA,
                BUCKET_COUNT_ATTRIBUTE_METADATA,
                SORT_ORDERS_ATTRIBUTE_METADATA,
                partitioningHandleTrinoAttributeMetadata,
                nullableValuesTrinoAttributeMetadata);
    }

    public enum ExchangeType
    {
        GATHER,
        REPARTITION,
        REPLICATE;

        public static ExchangeType of(ExchangeNode.Type type)
        {
            return switch (type) {
                case GATHER -> GATHER;
                case REPARTITION -> REPARTITION;
                case REPLICATE -> REPLICATE;
            };
        }
    }

    public enum ExchangeScope
    {
        LOCAL,
        REMOTE;

        public static ExchangeScope of(ExchangeNode.Scope scope)
        {
            return switch (scope) {
                case LOCAL -> LOCAL;
                case REMOTE -> REMOTE;
            };
        }
    }

    public record NullableValues(NullableValue[] nullableValues)
    {
        public NullableValues
        {
            requireNonNull(nullableValues, "nullableValues is null");
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == this) {
                return true;
            }
            if (obj == null || obj.getClass() != this.getClass()) {
                return false;
            }
            var that = (NullableValues) obj;
            return Arrays.equals(nullableValues, that.nullableValues);
        }

        @Override
        public int hashCode()
        {
            return Arrays.hashCode(this.nullableValues);
        }
    }
}
