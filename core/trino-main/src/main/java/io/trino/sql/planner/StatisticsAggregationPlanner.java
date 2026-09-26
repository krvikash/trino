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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.metadata.FunctionResolver;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.aggregation.MaxDataSizeForStats;
import io.trino.operator.aggregation.SumDataSizeForStats;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.statistics.ColumnStatisticMetadata;
import io.trino.spi.statistics.ColumnStatisticType;
import io.trino.spi.statistics.TableStatisticType;
import io.trino.spi.statistics.TableStatisticsMetadata;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.StatisticAggregations;
import io.trino.sql.planner.plan.StatisticAggregationsDescriptor;
import io.trino.sql.tree.QualifiedName;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.operator.aggregation.ApproximateSetGenericAggregation.APPROX_SET_FUNCTION_NAME;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.statistics.TableStatisticType.ROW_COUNT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static java.util.Objects.requireNonNull;

public class StatisticsAggregationPlanner
{
    private final SymbolAllocator symbolAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final Metadata metadata;
    private final Session session;
    private final FunctionResolver functionResolver;

    public StatisticsAggregationPlanner(SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, PlannerContext plannerContext, Session session)
    {
        this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.metadata = plannerContext.getMetadata();
        this.session = requireNonNull(session, "session is null");
        this.functionResolver = plannerContext.getFunctionResolver();
    }

    public TableStatisticAggregation createStatisticsAggregation(TableStatisticsMetadata statisticsMetadata, Map<ColumnFieldPath, Symbol> columnToSymbolMap)
    {
        StatisticAggregationsDescriptor.Builder<Symbol> descriptor = StatisticAggregationsDescriptor.builder();

        List<String> groupingColumns = statisticsMetadata.getGroupingColumns();
        List<Symbol> groupingSymbols = groupingColumns.stream()
                .map(column -> columnToSymbolMap.get(ColumnFieldPath.column(column)))
                .collect(toImmutableList());

        for (int i = 0; i < groupingSymbols.size(); i++) {
            descriptor.addGrouping(groupingColumns.get(i), groupingSymbols.get(i));
        }

        ImmutableMap.Builder<Symbol, AggregationNode.Aggregation> aggregations = ImmutableMap.builder();
        for (TableStatisticType type : statisticsMetadata.getTableStatistics()) {
            if (type != ROW_COUNT) {
                throw new TrinoException(NOT_SUPPORTED, "Table-wide statistic type not supported: " + type);
            }
            AggregationNode.Aggregation aggregation = new AggregationNode.Aggregation(
                    metadata.resolveBuiltinFunction(getCharVarcharCoercion(session), "count", ImmutableList.of()),
                    ImmutableList.of(),
                    false,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
            Symbol symbol = symbolAllocator.newSymbol("rowCount", BIGINT);
            aggregations.put(symbol, aggregation);
            descriptor.addTableStatistic(ROW_COUNT, symbol);
        }

        for (ColumnStatisticMetadata columnStatisticMetadata : statisticsMetadata.getColumnStatistics()) {
            ColumnFieldPath columnFieldPath = ColumnFieldPath.of(columnStatisticMetadata);
            String columnName = columnFieldPath.displayName();
            Symbol inputSymbol = columnToSymbolMap.get(columnFieldPath);
            verifyNotNull(inputSymbol, "no symbol for [%s] column, these columns exist: %s", columnName, columnToSymbolMap.keySet());
            Type inputType = inputSymbol.type();
            verifyNotNull(inputType, "inputType is null for symbol: %s", inputSymbol);
            ColumnStatisticsAggregation aggregation;
            String symbolHint;
            if (columnStatisticMetadata.getStatisticTypeIfPresent().isPresent()) {
                ColumnStatisticType statisticType = columnStatisticMetadata.getStatisticType();
                aggregation = createColumnAggregation(statisticType, inputSymbol, inputType);
                symbolHint = statisticType + ":" + columnName;
            }
            else {
                FunctionName aggregationName = columnStatisticMetadata.getAggregation();
                aggregation = createColumnAggregation(aggregationName, inputSymbol, inputType);
                symbolHint = aggregationName.getName() + ":" + columnName;
            }
            Symbol symbol = symbolAllocator.newSymbol(symbolHint, aggregation.getOutputType());
            aggregations.put(symbol, aggregation.getAggregation());
            descriptor.addColumnStatistic(columnStatisticMetadata, symbol);
        }

        StatisticAggregations aggregation = new StatisticAggregations(aggregations.buildOrThrow(), groupingSymbols);
        return new TableStatisticAggregation(aggregation, descriptor.build());
    }

    private ColumnStatisticsAggregation createColumnAggregation(ColumnStatisticType statisticType, Symbol input, Type inputType)
    {
        return switch (statisticType) {
            case MIN_VALUE -> createAggregation("min", input, inputType);
            case MAX_VALUE -> createAggregation("max", input, inputType);
            case NUMBER_OF_DISTINCT_VALUES -> createAggregation("approx_distinct", input, inputType);
            // we use $approx_set here and not approx_set because latter is not defined for all types supported by Trino
            case NUMBER_OF_DISTINCT_VALUES_SUMMARY -> createAggregation(APPROX_SET_FUNCTION_NAME, input, inputType);
            case NUMBER_OF_NON_NULL_VALUES -> createAggregation("count", input, inputType);
            case NUMBER_OF_TRUE_VALUES -> createAggregation("count_if", input, BOOLEAN);
            case TOTAL_SIZE_IN_BYTES -> createAggregation(SumDataSizeForStats.NAME, input, inputType);
            case MAX_VALUE_SIZE_IN_BYTES -> createAggregation(MaxDataSizeForStats.NAME, input, inputType);
        };
    }

    private ColumnStatisticsAggregation createColumnAggregation(FunctionName aggregation, Symbol input, Type inputType)
    {
        QualifiedName name = aggregation.getCatalogSchema()
                .map(catalogSchemaName -> QualifiedName.of(catalogSchemaName.getCatalogName(), catalogSchemaName.getSchemaName(), aggregation.getName()))
                .orElseGet(() -> QualifiedName.of(aggregation.getName()));
        // Statistics collection is part of the internal system, so it uses allow all access control
        return createAggregation(functionResolver.resolveFunction(session, name, fromTypes(inputType), new AllowAllAccessControl()), input, inputType);
    }

    private ColumnStatisticsAggregation createAggregation(String functionName, Symbol input, Type inputType)
    {
        return createAggregation(metadata.resolveBuiltinFunction(getCharVarcharCoercion(session), functionName, fromTypes(inputType)), input, inputType);
    }

    private static ColumnStatisticsAggregation createAggregation(ResolvedFunction resolvedFunction, Symbol input, Type inputType)
    {
        Type resolvedType = getOnlyElement(resolvedFunction.signature().getArgumentTypes());
        verify(resolvedType.equals(inputType), "resolved function input type does not match the input type: %s != %s", resolvedType, inputType);
        return new ColumnStatisticsAggregation(
                new AggregationNode.Aggregation(
                        resolvedFunction,
                        ImmutableList.of(input.toSymbolReference()),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                resolvedFunction.signature().getReturnType());
    }

    /**
     * A connector may request a statistic for a primitive leaf field nested inside a ROW column.
     * The aggregation can only consume a symbol, so materialise a dereference chain per requested
     * leaf in a {@link ProjectNode} over {@code source}, passing {@code source}'s own output
     * symbols through as identities. Dereference pushdown later folds the projection into the
     * scan where the connector supports it.
     * <p>
     * Returns {@code source} unchanged when no nested leaf is requested.
     */
    public StatisticsAggregationSource projectNestedStatisticsFields(
            PlanNode source,
            Map<String, Symbol> columnNameToSymbol,
            TableStatisticsMetadata statisticsMetadata)
    {
        Map<ColumnFieldPath, Symbol> columnToSymbolMap = new LinkedHashMap<>();
        columnNameToSymbol.forEach((columnName, symbol) -> columnToSymbolMap.put(ColumnFieldPath.column(columnName), symbol));

        Assignments.Builder nestedAssignments = Assignments.builder();
        for (ColumnStatisticMetadata columnStatistic : statisticsMetadata.getColumnStatistics()) {
            if (columnStatistic.getFieldPath().isEmpty()) {
                continue;
            }
            ColumnFieldPath columnFieldPath = ColumnFieldPath.of(columnStatistic);
            if (columnToSymbolMap.containsKey(columnFieldPath)) {
                // Several statistics may be requested for the same leaf
                continue;
            }
            Symbol baseSymbol = columnNameToSymbol.get(columnStatistic.getColumnName());
            verifyNotNull(baseSymbol, "no symbol for [%s] column, these columns exist: %s", columnStatistic.getColumnName(), columnNameToSymbol.keySet());
            Expression leafExpression = dereferenceNestedField(baseSymbol.toSymbolReference(), columnFieldPath);
            Symbol leafSymbol = symbolAllocator.newSymbol(columnFieldPath.displayName(), leafExpression.type());
            nestedAssignments.put(leafSymbol, leafExpression);
            columnToSymbolMap.put(columnFieldPath, leafSymbol);
        }

        Assignments assignments = nestedAssignments.build();
        if (assignments.isEmpty()) {
            return new StatisticsAggregationSource(source, columnToSymbolMap);
        }
        return new StatisticsAggregationSource(
                new ProjectNode(
                        idAllocator.getNextId(),
                        source,
                        Assignments.builder()
                                .putIdentities(source.getOutputSymbols())
                                .putAll(assignments)
                                .build()),
                columnToSymbolMap);
    }

    private static Expression dereferenceNestedField(Expression base, ColumnFieldPath columnFieldPath)
    {
        Expression expression = base;
        for (String fieldName : columnFieldPath.fieldPath()) {
            Type type = expression.type();
            verify(type instanceof RowType, "Statistic %s requires field [%s] of a row type, but found: %s", columnFieldPath.displayName(), fieldName, type);
            List<RowType.Field> fields = ((RowType) type).getFields();
            int index = IntStream.range(0, fields.size())
                    .filter(i -> fields.get(i).getName().equals(Optional.of(fieldName)))
                    .findFirst()
                    .orElse(-1);
            verify(index >= 0, "Statistic %s requires field [%s], but found: %s", columnFieldPath.displayName(), fieldName, type);
            expression = new FieldReference(expression, index);
        }
        return expression;
    }

    public record StatisticsAggregationSource(PlanNode source, Map<ColumnFieldPath, Symbol> columnToSymbolMap)
    {
        public StatisticsAggregationSource
        {
            requireNonNull(source, "source is null");
            columnToSymbolMap = ImmutableMap.copyOf(requireNonNull(columnToSymbolMap, "columnToSymbolMap is null"));
        }
    }

    /**
     * Identifies the input of a column statistic: a top-level column, or a primitive leaf field
     * nested inside one. The field path is part of the key because a dotted name alone is
     * ambiguous — a table may have a top-level column literally named {@code a.x} as well as a
     * column {@code a} with a field {@code x}.
     */
    public record ColumnFieldPath(String columnName, List<String> fieldPath)
    {
        public ColumnFieldPath
        {
            requireNonNull(columnName, "columnName is null");
            fieldPath = ImmutableList.copyOf(requireNonNull(fieldPath, "fieldPath is null"));
        }

        public static ColumnFieldPath column(String columnName)
        {
            return new ColumnFieldPath(columnName, ImmutableList.of());
        }

        public static ColumnFieldPath of(ColumnStatisticMetadata columnStatisticMetadata)
        {
            return new ColumnFieldPath(columnStatisticMetadata.getColumnName(), columnStatisticMetadata.getFieldPath());
        }

        /**
         * The column name with the field path appended, e.g. {@code attrs.intent}. For symbol names
         * and messages only, as a dotted name does not identify a field unambiguously.
         */
        public String displayName()
        {
            if (fieldPath.isEmpty()) {
                return columnName;
            }
            return columnName + "." + String.join(".", fieldPath);
        }
    }

    public static class TableStatisticAggregation
    {
        private final StatisticAggregations aggregations;
        private final StatisticAggregationsDescriptor<Symbol> descriptor;

        private TableStatisticAggregation(
                StatisticAggregations aggregations,
                StatisticAggregationsDescriptor<Symbol> descriptor)
        {
            this.aggregations = requireNonNull(aggregations, "aggregations is null");
            this.descriptor = requireNonNull(descriptor, "descriptor is null");
        }

        public StatisticAggregations getAggregations()
        {
            return aggregations;
        }

        public StatisticAggregationsDescriptor<Symbol> getDescriptor()
        {
            return descriptor;
        }
    }

    public static class ColumnStatisticsAggregation
    {
        private final AggregationNode.Aggregation aggregation;
        private final Type outputType;

        private ColumnStatisticsAggregation(AggregationNode.Aggregation aggregation, Type outputType)
        {
            this.aggregation = requireNonNull(aggregation, "aggregation is null");
            this.outputType = requireNonNull(outputType, "outputType is null");
        }

        public AggregationNode.Aggregation getAggregation()
        {
            return aggregation;
        }

        public Type getOutputType()
        {
            return outputType;
        }
    }
}
