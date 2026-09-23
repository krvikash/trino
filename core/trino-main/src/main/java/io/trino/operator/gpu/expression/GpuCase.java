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
package io.trino.operator.gpu.expression;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.Scalar;
import ai.rapids.cudf.Table;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.transform;
import static io.trino.operator.gpu.expression.CudfUtils.allTrue;
import static io.trino.operator.gpu.expression.CudfUtils.anyTrue;
import static io.trino.operator.gpu.expression.Fallibility.FALLIBLE_EXCEPT_ALL_NULLS;
import static io.trino.operator.gpu.expression.Fallibility.best;
import static io.trino.operator.gpu.expression.Fallibility.worst;
import static io.trino.operator.gpu.expression.GpuExpressions.worstOf;
import static io.trino.spi.gpu.GpuTypeConversion.nullScalar;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class GpuCase
        extends GpuExpression
{
    private final List<GpuExpression> conditions;
    private final List<GpuExpression> results;
    private final Type outputType;

    public GpuCase(List<GpuExpression> conditions, List<GpuExpression> results, Type outputType)
    {
        checkArgument(conditions.size() == results.size(), "conditions and results must have equal size");
        this.conditions = ImmutableList.copyOf(conditions);
        this.results = ImmutableList.copyOf(results);
        this.outputType = requireNonNull(outputType, "outputType is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        List<Entry<Integer, @Borrow ColumnVector>> allBindings = bindings.allBindings();
        List<@Borrow ColumnVector> allInputColumns = ImmutableList.<@Borrow ColumnVector>builder()
                .addAll(inputColumns)
                .addAll(transform(allBindings, Entry::getValue))
                .build();
        try (Scalar nullScalar = nullScalar(outputType);
                Scalar falseScalar = Scalar.fromBool(false);
                Scalar trueScalar = Scalar.fromBool(true);
                Scalar zeroScalar = Scalar.fromInt(0);
                ColumnVector sequence = ColumnVector.sequence(zeroScalar, positionCount);
                Table sequenceTable = new Table(sequence);
                ClosingRef<ColumnVector> output = ClosingRef.own(ColumnVector.fromScalar(nullScalar, positionCount));
                ClosingRef<ColumnVector> remainingMask = ClosingRef.own(ColumnVector.fromScalar(trueScalar, positionCount))) {
            for (int i = 0; i < conditions.size(); i++) {
                if (!anyTrue(remainingMask.borrow())) {
                    break;
                }

                try (Table remainingIndicesTable = sequenceTable.filter(remainingMask.borrow());
                        ClosingRef<Table> filteredInput = filter(allInputColumns, remainingMask.borrow())) {
                    int filteredRowCount = toIntExact(remainingIndicesTable.getRowCount());
                    List<@Borrow ColumnVector> filteredAll = getColumns(filteredInput);
                    List<@Borrow ColumnVector> filteredColumns = filteredAll.subList(0, inputColumns.size());
                    Bindings filteredBindings = zipBindings(allBindings, filteredAll.subList(inputColumns.size(), filteredAll.size()));

                    try (ColumnVector conditionRaw = conditions.get(i).evaluate(filteredRowCount, filteredColumns, filteredBindings);
                            ColumnVector conditionMask = conditionRaw.replaceNulls(falseScalar)) {
                        if (!anyTrue(conditionMask)) {
                            continue;
                        }

                        if (allTrue(conditionMask)) {
                            try (ColumnVector branchResult = results.get(i).evaluate(filteredRowCount, filteredColumns, filteredBindings)) {
                                output.replace(scatterCombine(output.borrow(), branchResult, remainingIndicesTable.getColumn(0)));
                            }
                            remainingMask.replace(ColumnVector.fromScalar(falseScalar, positionCount));
                            break;
                        }

                        try (Table matchedIndicesTable = remainingIndicesTable.filter(conditionMask);
                                ClosingRef<Table> matchedInput = filter(filteredInput, conditionMask)) {
                            int matchedRowCount = toIntExact(matchedIndicesTable.getRowCount());
                            List<@Borrow ColumnVector> matchedAll = getColumns(matchedInput);
                            List<@Borrow ColumnVector> matchedColumns = matchedAll.subList(0, inputColumns.size());
                            Bindings matchedBindings = zipBindings(allBindings, matchedAll.subList(inputColumns.size(), matchedAll.size()));

                            try (ColumnVector branchResult = results.get(i).evaluate(matchedRowCount, matchedColumns, matchedBindings)) {
                                output.replace(scatterCombine(output.borrow(), branchResult, matchedIndicesTable.getColumn(0)));
                            }
                            try (ColumnVector falseValues = ColumnVector.fromScalar(falseScalar, matchedRowCount)) {
                                remainingMask.replace(scatterCombine(remainingMask.borrow(), falseValues, matchedIndicesTable.getColumn(0)));
                            }
                        }
                    }
                }
            }

            return output.take();
        }
    }

    private static Bindings zipBindings(List<Entry<Integer, @Borrow ColumnVector>> bindings, List<@Borrow ColumnVector> boundColumns)
    {
        checkArgument(boundColumns.size() == bindings.size(), "Expected %s bound columns, got %s", bindings.size(), boundColumns.size());
        Bindings result = new Bindings();
        for (int i = 0; i < bindings.size(); i++) {
            result.put(bindings.get(i).getKey(), boundColumns.get(i));
        }
        return result;
    }

    private static @Move ClosingRef<Table> filter(List<@Borrow ColumnVector> inputColumns, @Borrow ColumnVector mask)
    {
        if (inputColumns.isEmpty()) {
            return ClosingRef.empty();
        }
        try (Table inputTable = new Table(inputColumns.toArray(ColumnVector[]::new))) {
            return ClosingRef.own(inputTable.filter(mask));
        }
    }

    private static @Move ClosingRef<Table> filter(@Borrow ClosingRef<Table> input, @Borrow ColumnVector mask)
    {
        if (input.isEmpty()) {
            return ClosingRef.empty();
        }
        return ClosingRef.own(input.borrow().filter(mask));
    }

    private static List<@Borrow ColumnVector> getColumns(@Borrow ClosingRef<Table> tableRef)
    {
        if (tableRef.isEmpty()) {
            return ImmutableList.of();
        }
        Table table = tableRef.borrow();
        int columnCount = table.getNumberOfColumns();
        ImmutableList.Builder<@Borrow ColumnVector> columns = ImmutableList.builderWithExpectedSize(columnCount);
        for (int i = 0; i < columnCount; i++) {
            columns.add(table.getColumn(i));
        }
        return columns.build();
    }

    private static @Move ColumnVector scatterCombine(@Borrow ColumnVector target, @Borrow ColumnVector source, @Borrow ColumnVector indices)
    {
        try (Table sourceTable = new Table(source);
                Table targetTable = new Table(target);
                Table scattered = sourceTable.scatter(indices, targetTable)) {
            return scattered.getColumn(0).incRefCount();
        }
    }

    @Override
    protected Fallibility computeFallibility()
    {
        Fallibility fallibility = worstOf(conditions);
        for (int i = 0; i < conditions.size(); i++) {
            Fallibility resultFallibility;
            if (conditions.get(i).nullOnNullInputs()) {
                // A NULL condition cannot select its result on an all-NULL input row.
                // The result can still fail on other rows, so cap its fallibility at FALLIBLE_EXCEPT_ALL_NULLS.
                resultFallibility = best(results.get(i).fallibility(), FALLIBLE_EXCEPT_ALL_NULLS);
            }
            else {
                resultFallibility = results.get(i).fallibility();
            }
            fallibility = worst(fallibility, resultFallibility);
        }
        return fallibility;
    }

    @Override
    protected boolean computeNullOnNullInputs()
    {
        // A result is selected only where its condition is TRUE, and where no condition is, the output stays NULL
        for (int i = 0; i < conditions.size(); i++) {
            if (!conditions.get(i).nullOnNullInputs() && !results.get(i).nullOnNullInputs()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuCase other
                && conditions.equals(other.conditions)
                && results.equals(other.results)
                && outputType.equals(other.outputType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), conditions, results, outputType);
    }
}
