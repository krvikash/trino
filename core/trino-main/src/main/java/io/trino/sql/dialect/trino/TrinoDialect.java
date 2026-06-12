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
package io.trino.sql.dialect.trino;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.trino.metadata.TableHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;
import io.trino.sql.dialect.trino.operationmetadata.AggregateCallOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.AggregationOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.ArrayOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.AssignUniqueIdOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.BetweenOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.BindOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.CallOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.CaseOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.CastOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.CoalesceOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.ComparisonOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.ConstantOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.CorrelatedJoinOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.DynamicFilterSourceOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.EnforceSingleRowOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.ExceptOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.ExchangeOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.ExplainAnalyzeOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.FieldReferenceOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.FilterOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.GroupIdOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.InOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.IntersectOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.IsNullOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.LambdaOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.LimitOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.LogicalOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.MatchOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.NullIfOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.OutputOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.ProjectOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.QueryOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.ReturnOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.RowOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.SemiJoinOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.SortOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.TableScanOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.TopNOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.TopNRankingOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.ConstantValue;
import io.trino.sql.dialect.trino.operationmetadata.TrinoOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.UnionOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.ValuesOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.WindowFunctionCallOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.WindowOperationMetadata;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Dialect;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Operation.AttributeKey;
import io.trino.sql.newir.Operation.OperationId;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Type;
import io.trino.sql.newir.Value;
import io.trino.sql.planner.PartitioningHandle;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.sql.dialect.trino.operationmetadata.ConstantOperationMetadata.CONSTANT_VALUE;
import static io.trino.sql.dialect.trino.operationmetadata.ExchangeOperationMetadata.CONSTANT_VALUES;
import static io.trino.sql.dialect.trino.operationmetadata.ExchangeOperationMetadata.PARTITIONING_HANDLE;
import static io.trino.sql.dialect.trino.operationmetadata.TableScanOperationMetadata.COLUMN_HANDLES;
import static io.trino.sql.dialect.trino.operationmetadata.TableScanOperationMetadata.CONSTRAINT;
import static io.trino.sql.dialect.trino.operationmetadata.TableScanOperationMetadata.TABLE_HANDLE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;

public final class TrinoDialect
        extends Dialect
{
    // dialect name
    public static final String TRINO = "trino";
    private static final Set<TrinoOperationMetadata> STATIC_OPERATIONS = staticOperations();
    public static final TrinoDialect TESTING_TRINO_DIALECT = new TrinoDialect();

    private final Map<String, TrinoOperationMetadata> operations;
    private final Map<String, TrinoAttributeMetadata<?>> attributes;
    private final Function<String, io.trino.spi.type.Type> typeDeserializer;

    @Inject
    public TrinoDialect(
            TypeManager typeManager,
            JsonCodec<ConstantValue> constantValueCodec,
            JsonCodec<PartitioningHandle> partitioningHandleCodec,
            JsonCodec<ConstantValue[]> constantValueArrayCodec,
            JsonCodec<TableHandle> tableHandleCodec,
            JsonCodec<List<ColumnHandle>> columnHandleCodec,
            JsonCodec<TupleDomain<ColumnHandle>> tupleDomainCodec)
    {
        super(TRINO);
        requireNonNull(typeManager, "typeManager is null");
        requireNonNull(constantValueCodec, "constantValueCodec is null");
        requireNonNull(partitioningHandleCodec, "partitioningHandleCodec is null");
        requireNonNull(constantValueArrayCodec, "constantValueArrayCodec is null");
        requireNonNull(tableHandleCodec, "tableHandleCodec is null");
        requireNonNull(columnHandleCodec, "columnHandleCodec is null");
        requireNonNull(tupleDomainCodec, "tupleDomainCodec is null");

        this.typeDeserializer = serializedType -> typeManager.getType(TypeId.of(serializedType));

        List<TrinoOperationMetadata> operationMetadata = ImmutableList.of(
                new AggregateCallOperationMetadata(typeDeserializer),
                new ArrayOperationMetadata(typeDeserializer),
                new CastOperationMetadata(typeDeserializer),
                new ConstantOperationMetadata(constantValueCodec),
                new ExchangeOperationMetadata(partitioningHandleCodec, constantValueArrayCodec),
                new TableScanOperationMetadata(tableHandleCodec, columnHandleCodec, tupleDomainCodec, typeDeserializer),
                new ValuesOperationMetadata(typeDeserializer));

        ImmutableMap.Builder<String, TrinoOperationMetadata> operationsBuilder = ImmutableMap.builder();
        STATIC_OPERATIONS.forEach(operation -> operationsBuilder.put(operation.name(), operation));
        operationMetadata.forEach(operation -> operationsBuilder.put(operation.name(), operation));
        this.operations = operationsBuilder.buildOrThrow();

        this.attributes = this.operations.values().stream()
                .peek(TrinoDialect::validateInternalNamespacedAttributes)
                .map(TrinoOperationMetadata::operationAttributes)
                .flatMap(Set::stream)
                .collect(toImmutableMap(attribute -> attribute.trinoAttributeSignature().name(), identity()));
    }

    private TrinoDialect()
    {
        super(TRINO);

        this.typeDeserializer = serializedType -> {
            throw new UnsupportedOperationException("cannot parse type " + serializedType);
        };

        List<TrinoOperationMetadata> operationMetadata = ImmutableList.of(
                new AggregateCallOperationMetadata(typeDeserializer),
                new ArrayOperationMetadata(typeDeserializer),
                new CastOperationMetadata(typeDeserializer),
                new ConstantOperationMetadata(
                        _ -> {
                            throw new UnsupportedOperationException(format("cannot parse %s attribute", CONSTANT_VALUE.name()));
                        },
                        ConstantValue::toString),
                new ExchangeOperationMetadata(
                        _ -> {
                            throw new UnsupportedOperationException(format("cannot parse %s attribute", PARTITIONING_HANDLE.name()));
                        },
                        PartitioningHandle::toString,
                        _ -> {
                            throw new UnsupportedOperationException(format("cannot parse %s attribute", CONSTANT_VALUES.name()));
                        },
                        constantValues -> Arrays.stream(constantValues.constantValues())
                                .map(Objects::toString)
                                .collect(joining(",", "[", "]"))),
                new TableScanOperationMetadata(
                        _ -> {
                            throw new UnsupportedOperationException(format("cannot parse %s attribute", TABLE_HANDLE.name()));
                        },
                        TableHandle::toString,
                        _ -> {
                            throw new UnsupportedOperationException(format("cannot parse %s attribute", COLUMN_HANDLES.name()));
                        },
                        columnHandles -> Joiner.on(", ").join(columnHandles),
                        _ -> {
                            throw new UnsupportedOperationException(format("cannot parse %s attribute", CONSTRAINT.name()));
                        },
                        TupleDomain::toString,
                        typeDeserializer),
                new ValuesOperationMetadata(typeDeserializer));

        ImmutableMap.Builder<String, TrinoOperationMetadata> operationsBuilder = ImmutableMap.builder();
        STATIC_OPERATIONS.forEach(operation -> operationsBuilder.put(operation.name(), operation));
        operationMetadata.forEach(operation -> operationsBuilder.put(operation.name(), operation));
        this.operations = operationsBuilder.buildOrThrow();

        this.attributes = this.operations.values().stream()
                .peek(TrinoDialect::validateInternalNamespacedAttributes)
                .map(TrinoOperationMetadata::operationAttributes)
                .flatMap(Set::stream)
                .collect(toImmutableMap(attribute -> attribute.trinoAttributeSignature().name(), identity()));
    }

    private static void validateInternalNamespacedAttributes(TrinoOperationMetadata operation)
    {
        String operationName = operation.name();
        for (TrinoAttributeMetadata<?> attribute : operation.operationAttributes()) {
            String attributeName = attribute.trinoAttributeSignature().name();
            if (!attributeName.startsWith(operationName + ":")) {
                throw new TrinoException(IR_ERROR, format("the name of a proper operation attribute: %s must be namespaced with the operation name: %s", attributeName, operationName));
            }
            if (attribute.trinoAttributeSignature().external()) {
                throw new TrinoException(IR_ERROR, format("proper operation attribute: %s cannot be external", attributeName));
            }
        }
    }

    @Override
    public String formatAttribute(String name, Object attribute)
    {
        TrinoAttributeMetadata<?> attributeMetadata = attributes.get(name);
        if (attributeMetadata == null) {
            throw new TrinoException(IR_ERROR, format("attribute %s not registered", name));
        }
        return attributeMetadata.print(attribute);
    }

    @Override
    public Object parseAttribute(String name, String attribute)
    {
        TrinoAttributeMetadata<?> attributeMetadata = attributes.get(name);
        if (attributeMetadata == null) {
            throw new TrinoException(IR_ERROR, format("attribute %s not registered", name));
        }
        return attributeMetadata.parse(attribute);
    }

    @Override
    public String formatType(Type type)
    {
        return trinoType(type).getTypeId().getId();
    }

    @Override
    public Type parseType(String type)
    {
        return irType(typeDeserializer.apply(type));
    }

    @Override
    public BiFunction<Attributes, List<Attributes>, Attributes> getAttributeDerivationForOperation(OperationId id)
    {
        // there are no overloads, so we can identify operation by name only
        TrinoOperationMetadata operationMetadata = operations.get(id.name());
        // TODO verify argumentTypes and regionTypes
        return operationMetadata.attributeDerivation();
    }

    @Override
    public Set<AttributeKey> getInherentOperationAttributeKeys(OperationId id)
    {
        // there are no overloads, so we can identify operation by name only
        TrinoOperationMetadata operationMetadata = operations.get(id.name());
        return operationMetadata.inherentOperationAttributeKeys();
    }

    @Override
    public Operation createOperation(String name, String resultName, List<Value> arguments, List<Region> regions, Attributes attributes)
    {
        // there are no overloads, so we can identify operation by name only
        TrinoOperationMetadata operationMetadata = operations.get(name);
        return operationMetadata.createOperation(resultName, arguments, regions, attributes);
    }

    @Override
    public Attributes deriveGroupAttributes(Attributes currentGroupAttributes, List<Attributes> operationsAttributes)
    {
        // Trino dialect does not support any derived attributes yet.
        return Attributes.empty();
    }

    @Override
    public Attributes mergeGroupAttributes(Attributes firstGroupAttributes, Attributes secondGroupAttributes)
    {
        // Trino dialect does not support any derived attributes yet.
        return Attributes.empty();
    }

    @Override
    public Attributes composeOperationAttributes(Attributes operationAttributes, Attributes groupAttributes)
    {
        // Trino dialect does not support any derived attributes yet.
        return Attributes.empty();
    }

    @Override
    public Attributes updateOperationAttributes(Attributes operationAttributes, Attributes derivedAttributes)
    {
        // Trino dialect does not support any derived attributes yet.
        return Attributes.empty();
    }

    private static Set<TrinoOperationMetadata> staticOperations()
    {
        return ImmutableSet.of(
                new AggregationOperationMetadata(),
                new AssignUniqueIdOperationMetadata(),
                new BetweenOperationMetadata(),
                new BindOperationMetadata(),
                new CallOperationMetadata(),
                new CaseOperationMetadata(),
                new CoalesceOperationMetadata(),
                new ComparisonOperationMetadata(),
                new CorrelatedJoinOperationMetadata(),
                new DynamicFilterSourceOperationMetadata(),
                new EnforceSingleRowOperationMetadata(),
                new ExceptOperationMetadata(),
                new ExplainAnalyzeOperationMetadata(),
                new FieldReferenceOperationMetadata(),
                new FilterOperationMetadata(),
                new GroupIdOperationMetadata(),
                new InOperationMetadata(),
                new IntersectOperationMetadata(),
                new IsNullOperationMetadata(),
                new JoinOperationMetadata(),
                new LambdaOperationMetadata(),
                new LimitOperationMetadata(),
                new LogicalOperationMetadata(),
                new MatchOperationMetadata(),
                new NullIfOperationMetadata(),
                new OutputOperationMetadata(),
                new ProjectOperationMetadata(),
                new QueryOperationMetadata(),
                new ReturnOperationMetadata(),
                new RowOperationMetadata(),
                new SemiJoinOperationMetadata(),
                new SortOperationMetadata(),
                new TopNOperationMetadata(),
                new TopNRankingOperationMetadata(),
                new UnionOperationMetadata(),
                new WindowFunctionCallOperationMetadata(),
                new WindowOperationMetadata());
    }

    public static io.trino.spi.type.Type trinoType(Type type)
    {
        if (!type.dialect().equals(TRINO)) {
            throw new TrinoException(IR_ERROR, format("expected a type of the %s dialect, actual dialect: %s", TRINO, type.dialect()));
        }

        if (type.dialectType() instanceof io.trino.spi.type.Type trinoType) {
            return trinoType;
        }

        throw new TrinoException(IR_ERROR, "expected a trino type, actual: " + type.dialectType().getClass().getSimpleName());
    }

    public static Type irType(io.trino.spi.type.Type type)
    {
        return new Type(TRINO, type);
    }
}
