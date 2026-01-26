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
import io.airlift.json.JsonCodec;
import io.trino.sql.dialect.ir.IrAttributeUtils;
import io.trino.sql.dialect.trino.operation.Exchange;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.ConstantValue;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.SortOrderList;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.TrinoAttributeSignature;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Operation.AttributeKey;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.plan.ExchangeNode;
import org.assertj.core.util.VisibleForTesting;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.dialect.ir.IrAttributeUtils.deterministic;
import static io.trino.sql.dialect.ir.IrAttributeUtils.hasNoSideEffects;
import static io.trino.sql.dialect.ir.IrAttributeUtils.hasSideEffects;
import static io.trino.sql.dialect.ir.IrAttributeUtils.safe;
import static io.trino.sql.dialect.ir.IrAttributeUtils.unsafe;
import static io.trino.sql.dialect.trino.operation.TrinoOperation.emptySourceAttributes;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalBooleanAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalEnumAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalIntegerAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalIntegerListAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalSortOrderListAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.prefixedName;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.partitioningBy;

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
    public static final TrinoAttributeSignature<ConstantValues> CONSTANT_VALUES = new TrinoAttributeSignature<>(prefixedName(NAME, "constant_values"), false);

    public static final Set<TrinoAttributeSignature<?>> OPERATION_ATTRIBUTES = ImmutableSet.of(
            EXCHANGE_TYPE,
            EXCHANGE_SCOPE,
            REPLICATE_NULLS_AND_ANY,
            BUCKET_TO_PARTITION,
            PARTITION_COUNT,
            BUCKET_COUNT,
            SORT_ORDERS,
            PARTITIONING_HANDLE,
            CONSTANT_VALUES);

    private final TrinoAttributeMetadata<PartitioningHandle> partitioningHandleTrinoAttributeMetadata;
    private final TrinoAttributeMetadata<ConstantValues> constantValuesTrinoAttributeMetadata;

    public ExchangeOperationMetadata(JsonCodec<PartitioningHandle> partitioningHandleCodec, JsonCodec<ConstantValue[]> constantValueArrayCodec)
    {
        this(
                partitioningHandleCodec::fromJson,
                partitioningHandleCodec::toJson,
                string -> new ConstantValues(constantValueArrayCodec.fromJson(string)),
                constantValues -> constantValueArrayCodec.toJson(constantValues.constantValues()));
    }

    @VisibleForTesting
    public ExchangeOperationMetadata(
            Function<String, PartitioningHandle> partitioningHandleParseMethod,
            Function<PartitioningHandle, String> partitioningHandlePrintMethod,
            Function<String, ConstantValues> constantValuesParseMethod,
            Function<ConstantValues, String> constantValuesPrintMethod)
    {
        requireNonNull(partitioningHandleParseMethod, "partitioningHandleParseMethod is null");
        requireNonNull(partitioningHandlePrintMethod, "partitioningHandlePrintMethod is null");
        requireNonNull(constantValuesParseMethod, "constantValuesParseMethod is null");
        requireNonNull(constantValuesPrintMethod, "constantValuesPrintMethod is null");

        this.partitioningHandleTrinoAttributeMetadata = new TrinoAttributeMetadata<>(PARTITIONING_HANDLE, partitioningHandleParseMethod, partitioningHandlePrintMethod);
        this.constantValuesTrinoAttributeMetadata = new TrinoAttributeMetadata<>(CONSTANT_VALUES, constantValuesParseMethod, constantValuesPrintMethod);
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
                constantValuesTrinoAttributeMetadata);
    }

    @Override
    public Operation createOperation(String resultName, List<Value> arguments, List<Region> regions, Map<AttributeKey, Object> attributes)
    {
        checkArgument(regions.size() == arguments.size() + 2, "The number of regions Exchange operation must be equal to the number of arguments plus two: one for partitioning bound arguments and one for sorting keys");

        Map<Boolean, List<Map.Entry<AttributeKey, Object>>> partitionedAttributes = attributes.entrySet().stream()
                .collect(partitioningBy(entry -> operationAttributeKeys().contains(entry.getKey())));
        Map<AttributeKey, Object> operationAttributes = ImmutableMap.copyOf(partitionedAttributes.get(true));
        Map<AttributeKey, Object> derivedAttributes = ImmutableMap.copyOf(partitionedAttributes.get(false));

        return new Exchange(
                resultName,
                arguments,
                regions.subList(0, arguments.size()).stream()
                        .map(Region::getOnlyBlock)
                        .map(block -> block.withLabel("^inputSelector"))
                        .collect(toImmutableList()),
                regions.get(regions.size() - 2).getOnlyBlock().withLabel("^boundArguments"),
                regions.getLast().getOnlyBlock().withLabel("^orderingSelector"),
                EXCHANGE_TYPE.getAttribute(operationAttributes),
                EXCHANGE_SCOPE.getAttribute(operationAttributes),
                PARTITIONING_HANDLE.getAttribute(operationAttributes),
                CONSTANT_VALUES.getAttribute(operationAttributes),
                REPLICATE_NULLS_AND_ANY.getAttribute(operationAttributes),
                Optional.ofNullable(BUCKET_TO_PARTITION.getAttribute(operationAttributes)),
                Optional.ofNullable(PARTITION_COUNT.getAttribute(operationAttributes)).map(OptionalInt::of).orElse(OptionalInt.empty()),
                Optional.ofNullable(BUCKET_COUNT.getAttribute(operationAttributes)).map(OptionalInt::of).orElse(OptionalInt.empty()),
                Optional.ofNullable(SORT_ORDERS.getAttribute(operationAttributes)),
                emptySourceAttributes(arguments.size()),
                derivedAttributes);
    }

    @Override
    public BiFunction<Map<AttributeKey, Object>, List<Map<AttributeKey, Object>>, Map<AttributeKey, Object>> attributeDerivation()
    {
        return ExchangeOperationMetadata::deriveAttributes;
    }

    public static Map<AttributeKey, Object> deriveAttributes(Map<AttributeKey, Object> currentAttributes, List<Map<AttributeKey, Object>> childAttributes)
    {
        ImmutableMap.Builder<AttributeKey, Object> derivedAttributes = ImmutableMap.builder();

        // For repeatability, we ignore the last two child attributes which correspond to the partitioning bound arguments and sorting keys.
        // They only affect how the data is organized on the physical level (partitioning and ordering).
        // The actual data passed through the exchange depends on the inputs and input field selectors, represented by the preceding child attributes.
        // TODO we can be more precise about deriving IR-level attributes if we
        //  - capture input attributes on the field level (e.g., repeatability of input columns)
        //  - analyze which fields are being selected by input field selectors (e.g., a non-deterministic column is not being passed through the exchange)
        if (childAttributes.subList(0, childAttributes.size() - 2).stream().allMatch(IrAttributeUtils::isKnownDeterministic)) {
            deterministic(derivedAttributes);
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

    public record ConstantValues(ConstantValue[] constantValues)
    {
        public ConstantValues
        {
            requireNonNull(constantValues, "constantValues is null");
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
            var that = (ConstantValues) obj;
            return Arrays.equals(constantValues, that.constantValues);
        }

        @Override
        public int hashCode()
        {
            return Arrays.hashCode(this.constantValues);
        }
    }
}
