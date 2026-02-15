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
import io.airlift.json.JsonCodecFactory;
import io.trino.cost.SymbolStatsEstimate;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.operation.TableScan;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.TrinoAttributeSignature;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Operation.AttributeKey;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;
import org.assertj.core.util.VisibleForTesting;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.dialect.ir.IrAttributeUtils.hasNoSideEffects;
import static io.trino.sql.dialect.ir.IrAttributeUtils.safe;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalBooleanAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.internalObjectAttributeMetadata;
import static io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.prefixedName;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.partitioningBy;

public class TableScanOperationMetadata
        implements TrinoOperationMetadata
{
    public static final String NAME = "table_scan";

    private static final TrinoAttributeMetadata<Statistics> STATISTICS_ATTRIBUTE_METADATA = internalObjectAttributeMetadata(NAME, "statistics", Statistics.STATISTICS_CODEC);
    private static final TrinoAttributeMetadata<Boolean> UPDATE_TARGET_ATTRIBUTE_METADATA = internalBooleanAttributeMetadata(NAME, "update_target");
    private static final TrinoAttributeMetadata<Boolean> USE_CONNECTOR_NODE_PARTITIONING_ATTRIBUTE_METADATA = internalBooleanAttributeMetadata(NAME, "use_connector_node_partitioning");

    public static final TrinoAttributeSignature<Statistics> STATISTICS = STATISTICS_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<Boolean> UPDATE_TARGET = UPDATE_TARGET_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<Boolean> USE_CONNECTOR_NODE_PARTITIONING = USE_CONNECTOR_NODE_PARTITIONING_ATTRIBUTE_METADATA.trinoAttributeSignature();
    public static final TrinoAttributeSignature<TableHandle> TABLE_HANDLE = new TrinoAttributeSignature<>(prefixedName(NAME, "table_handle"), false);
    public static final TrinoAttributeSignature<List<ColumnHandle>> COLUMN_HANDLES = new TrinoAttributeSignature<>(prefixedName(NAME, "column_handles"), false);
    public static final TrinoAttributeSignature<TupleDomain<ColumnHandle>> CONSTRAINT = new TrinoAttributeSignature<>(prefixedName(NAME, "constraint"), false);
    public static final TrinoAttributeSignature<Type> ROW_TYPE = new TrinoAttributeSignature<>(prefixedName(NAME, "row_type"), false);

    public static final Set<TrinoAttributeSignature<?>> OPERATION_ATTRIBUTES = ImmutableSet.of(
            STATISTICS,
            UPDATE_TARGET,
            USE_CONNECTOR_NODE_PARTITIONING,
            TABLE_HANDLE,
            COLUMN_HANDLES,
            CONSTRAINT,
            ROW_TYPE);

    private final TrinoAttributeMetadata<TableHandle> tableHandleTrinoAttributeMetadata;
    private final TrinoAttributeMetadata<List<ColumnHandle>> columnHandlesTrinoAttributeMetadata;
    private final TrinoAttributeMetadata<TupleDomain<ColumnHandle>> constraintTrinoAttributeMetadata;
    private final TrinoAttributeMetadata<Type> rowTypeTrinoAttributeMetadata;

    public TableScanOperationMetadata(JsonCodec<TableHandle> tableHandleCodec, JsonCodec<List<ColumnHandle>> columnHandleCodec, JsonCodec<TupleDomain<ColumnHandle>> tupleDomainCodec, Function<String, Type> typeDeserializer)
    {
        this(
                tableHandleCodec::fromJson,
                tableHandleCodec::toJson,
                columnHandleCodec::fromJson,
                columnHandleCodec::toJson,
                tupleDomainCodec::fromJson,
                tupleDomainCodec::toJson,
                typeDeserializer);
    }

    @VisibleForTesting
    public TableScanOperationMetadata(
            Function<String, TableHandle> tableHandleParseMethod,
            Function<TableHandle, String> tableHandlePrintMethod,
            Function<String, List<ColumnHandle>> columnHandlesParseMethod,
            Function<List<ColumnHandle>, String> columnHandlesPrintMethod,
            Function<String, TupleDomain<ColumnHandle>> constraintParseMethod,
            Function<TupleDomain<ColumnHandle>, String> constraintPrintMethod,
            Function<String, Type> typeDeserializer)
    {
        requireNonNull(tableHandleParseMethod, "tableHandleParseMethod is null");
        requireNonNull(tableHandlePrintMethod, "tableHandlePrintMethod is null");
        requireNonNull(columnHandlesParseMethod, "columnHandlesParseMethod is null");
        requireNonNull(columnHandlesPrintMethod, "columnHandlesPrintMethod is null");
        requireNonNull(constraintParseMethod, "constraintParseMethod is null");
        requireNonNull(constraintPrintMethod, "constraintPrintMethod is null");
        requireNonNull(typeDeserializer, "typeDeserializer is null");

        this.tableHandleTrinoAttributeMetadata = new TrinoAttributeMetadata<>(TABLE_HANDLE, tableHandleParseMethod, tableHandlePrintMethod);
        this.columnHandlesTrinoAttributeMetadata = new TrinoAttributeMetadata<>(COLUMN_HANDLES, columnHandlesParseMethod, columnHandlesPrintMethod);
        this.constraintTrinoAttributeMetadata = new TrinoAttributeMetadata<>(CONSTRAINT, constraintParseMethod, constraintPrintMethod);
        this.rowTypeTrinoAttributeMetadata = new TrinoAttributeMetadata<>(ROW_TYPE, typeDeserializer, type -> type.getTypeId().getId());
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
                STATISTICS_ATTRIBUTE_METADATA,
                UPDATE_TARGET_ATTRIBUTE_METADATA,
                USE_CONNECTOR_NODE_PARTITIONING_ATTRIBUTE_METADATA,
                tableHandleTrinoAttributeMetadata,
                columnHandlesTrinoAttributeMetadata,
                constraintTrinoAttributeMetadata,
                rowTypeTrinoAttributeMetadata);
    }

    @Override
    public Operation createOperation(String resultName, List<Value> arguments, List<Region> regions, Map<AttributeKey, Object> attributes)
    {
        checkArgument(arguments.isEmpty(), "TableScan operation does not have arguments");
        checkArgument(regions.isEmpty(), "TableScan operation does not have regions");

        Map<Boolean, List<Map.Entry<AttributeKey, Object>>> partitionedAttributes = attributes.entrySet().stream()
                .collect(partitioningBy(entry -> inherentOperationAttributeKeys().contains(entry.getKey())));
        Map<AttributeKey, Object> operationAttributes = ImmutableMap.copyOf(partitionedAttributes.get(true));
        Map<AttributeKey, Object> derivedAttributes = ImmutableMap.copyOf(partitionedAttributes.get(false));

        return new TableScan(
                resultName,
                ROW_TYPE.getAttribute(operationAttributes),
                TABLE_HANDLE.getAttribute(operationAttributes),
                COLUMN_HANDLES.getAttribute(operationAttributes),
                CONSTRAINT.getAttribute(operationAttributes),
                Optional.ofNullable(STATISTICS.getAttribute(operationAttributes)),
                UPDATE_TARGET.getAttribute(operationAttributes),
                Optional.ofNullable(USE_CONNECTOR_NODE_PARTITIONING.getAttribute(operationAttributes)),
                derivedAttributes);
    }

    @Override
    public BiFunction<Map<AttributeKey, Object>, List<Map<AttributeKey, Object>>, Map<AttributeKey, Object>> attributeDerivation()
    {
        return TableScanOperationMetadata::deriveAttributes;
    }

    public static Map<AttributeKey, Object> deriveAttributes(Map<AttributeKey, Object> currentAttributes, List<Map<AttributeKey, Object>> childAttributes)
    {
        checkArgument(childAttributes.isEmpty(), "TableScan operation must have exactly zero child attributes maps");

        ImmutableMap.Builder<AttributeKey, Object> derivedAttributes = ImmutableMap.builder();
        // TODO derive repeatability, for now we set unknown
        safe(derivedAttributes);
        hasNoSideEffects(derivedAttributes);

        return derivedAttributes.buildOrThrow();
    }

    public record Statistics(double outputRowCount, PMap<Integer, SymbolStatsEstimate> fieldStatistics)
    {
        public static final JsonCodec<Statistics> STATISTICS_CODEC = new JsonCodecFactory().jsonCodec(Statistics.class);

        public Statistics
        {
            requireNonNull(fieldStatistics, "fieldStatistics is null");
        }

        public Statistics(double outputRowCount, Map<Integer, SymbolStatsEstimate> fieldStatistics)
        {
            this(outputRowCount, HashTreePMap.from(requireNonNull(fieldStatistics, "fieldStatistics is null")));
        }
    }
}
