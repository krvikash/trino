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
import io.trino.plugin.base.gpu.ClosingOnce;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;

import static com.google.common.collect.Lists.transform;
import static io.trino.operator.gpu.expression.CudfUtils.allTrue;
import static io.trino.operator.gpu.expression.CudfUtils.anyTrue;
import static io.trino.operator.gpu.expression.Fallibility.FALLIBLE_EXCEPT_ALL_NULLS;
import static io.trino.operator.gpu.expression.Fallibility.best;
import static io.trino.operator.gpu.expression.Fallibility.worst;
import static io.trino.operator.gpu.expression.GpuExpressions.allNullOnNullInputs;
import static io.trino.operator.gpu.expression.GpuExpressions.worstOf;
import static io.trino.spi.gpu.GpuTypeConversion.nullScalar;
import static java.util.Objects.requireNonNull;

/**
 * {@code IF(condition, trueValue, falseValue)} with lazy evaluation semantics.
 *
 * <p>Each branch is evaluated only on the rows where it is selected, so a
 * sub-expression that the CPU would never have evaluated for a given row never
 * triggers a runtime check on the GPU either. For example, the true branch of
 * {@code IF(b != 0, 10 / coalesce(b, 0), 42)} only sees rows where
 * {@code b != 0}, so the divide-by-zero check never fires — even though
 * {@code coalesce} would otherwise resurrect the zero divisor from any
 * NULL-masked input.
 */
public final class GpuIf
        extends GpuExpression
{
    private final GpuExpression condition;
    private final GpuExpression trueValue;
    private final GpuExpression falseValue;
    private final Type outputType;

    public GpuIf(GpuExpression condition, GpuExpression trueValue, GpuExpression falseValue, Type outputType)
    {
        this.condition = requireNonNull(condition, "condition is null");
        this.trueValue = requireNonNull(trueValue, "trueValue is null");
        this.falseValue = requireNonNull(falseValue, "falseValue is null");
        this.outputType = requireNonNull(outputType, "outputType is null");
    }

    @Override
    public @Move ColumnVector evaluate(int positionCount, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        try (ClosingOnce<ColumnVector> conditionRaw = ClosingOnce.own(condition.evaluate(positionCount, inputColumns, bindings));
                Scalar falseScalar = Scalar.fromBool(false);
                // Treat NULL as FALSE so the false branch covers it, matching Trino IF semantics.
                ClosingOnce<ColumnVector> conditionMask = ClosingOnce.own(conditionRaw.borrow().replaceNulls(falseScalar))) {
            conditionRaw.close();
            // Every row selects the true branch — skip filter+scatter (and the unused negated mask).
            if (allTrue(conditionMask.borrow())) {
                return trueValue.evaluate(positionCount, inputColumns, bindings);
            }
            if (!anyTrue(conditionMask.borrow())) {
                return falseValue.evaluate(positionCount, inputColumns, bindings);
            }
            // TODO (https://starburstdata.atlassian.net/browse/ENG-12126) when both branches are infallible, evaluate them eagerly and combine with ifElse instead of filter+scatter.
            // TODO (https://starburstdata.atlassian.net/browse/ENG-12126) when both branches are infallible on all-NULL input, evaluate them with masked inputs instead of filter+scatter.
            try (Scalar zero = Scalar.fromInt(0);
                    ColumnVector sequence = ColumnVector.sequence(zero, positionCount);
                    Table sequenceTable = new Table(sequence)) {
                try (ColumnVector trueResult = filterAndEvaluate(trueValue, conditionMask.borrow(), inputColumns, bindings);
                        Table trueIndices = sequenceTable.filter(conditionMask.borrow());
                        ColumnVector notConditionMask = conditionMask.borrow().not()) {
                    conditionMask.close();
                    try (ColumnVector falseResult = filterAndEvaluate(falseValue, notConditionMask, inputColumns, bindings);
                            Table falseIndices = sequenceTable.filter(notConditionMask)) {
                        return scatterCombine(outputType, positionCount, trueIndices.getColumn(0), falseIndices.getColumn(0), trueResult, falseResult);
                    }
                }
            }
        }
    }

    private static @Move ColumnVector filterAndEvaluate(
            GpuExpression branch,
            @Borrow ColumnVector mask,
            List<@Borrow ColumnVector> inputColumns,
            Bindings bindings)
    {
        List<Entry<Integer, @Borrow ColumnVector>> allBindings = bindings.allBindings();
        List<@Borrow ColumnVector> allInputColumns = ImmutableList.<@Borrow ColumnVector>builder()
                .addAll(inputColumns)
                .addAll(transform(allBindings, Entry::getValue))
                .build();
        // Table requires at least one column; if the IF references no input fields, use the
        // mask itself as a placeholder so we can still derive the filtered row count.
        @Borrow ColumnVector[] tableColumns = allInputColumns.isEmpty()
                ? new ColumnVector[] {mask}
                : allInputColumns.toArray(ColumnVector[]::new);
        try (Table inputTable = new Table(tableColumns);
                Table filtered = inputTable.filter(mask)) {
            int filteredRowCount = (int) filtered.getRowCount();
            ImmutableList.Builder<@Borrow ColumnVector> filteredColumns = ImmutableList.builderWithExpectedSize(inputColumns.size());
            for (int i = 0; i < inputColumns.size(); i++) {
                filteredColumns.add(filtered.getColumn(i));
            }
            Bindings filteredBindings = new Bindings();
            for (int i = 0; i < allBindings.size(); i++) {
                filteredBindings.put(allBindings.get(i).getKey(), filtered.getColumn(inputColumns.size() + i));
            }
            return branch.evaluate(filteredRowCount, filteredColumns.build(), filteredBindings);
        }
    }

    /**
     * Combines per-branch results computed over disjoint subsets of the original rows into a
     * single full-length output column.
     */
    private static @Move ColumnVector scatterCombine(
            Type outputType,
            int positionCount,
            @Borrow ColumnVector trueIndices,
            @Borrow ColumnVector falseIndices,
            @Borrow ColumnVector trueResult,
            @Borrow ColumnVector falseResult)
    {
        // Start with an all-NULL column of `positionCount` rows, then run two scatters: the first
        // writes `trueResult` rows into the positions listed in `trueIndices`, the second writes
        // `falseResult` rows into the remaining positions (`falseIndices`). Because every original
        // position is in exactly one of the two index lists, the second scatter overwrites every
        // remaining NULL and the final column has no leftover NULLs from the initial target.
        try (Scalar nullScalar = nullScalar(outputType);
                ColumnVector initialTarget = ColumnVector.fromScalar(nullScalar, positionCount);
                Table initialTable = new Table(initialTarget);
                Table trueResultTable = new Table(trueResult);
                Table partial = trueResultTable.scatter(trueIndices, initialTable);
                Table falseResultTable = new Table(falseResult);
                Table finalTable = falseResultTable.scatter(falseIndices, partial)) {
            return finalTable.getColumn(0).incRefCount();
        }
    }

    @Override
    protected Fallibility computeFallibility()
    {
        Fallibility trueValueFallibility;
        if (condition.nullOnNullInputs()) {
            // A NULL condition selects the false branch
            trueValueFallibility = best(trueValue.fallibility(), FALLIBLE_EXCEPT_ALL_NULLS);
        }
        else {
            trueValueFallibility = trueValue.fallibility();
        }
        return worst(worstOf(ImmutableList.of(condition, falseValue)), trueValueFallibility);
    }

    @Override
    protected boolean computeNullOnNullInputs()
    {
        if (condition.nullOnNullInputs()) {
            // A NULL condition selects the false branch
            return falseValue.nullOnNullInputs();
        }
        return allNullOnNullInputs(ImmutableList.of(trueValue, falseValue));
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof GpuIf other
                && condition.equals(other.condition)
                && trueValue.equals(other.trueValue)
                && falseValue.equals(other.falseValue)
                && outputType.equals(other.outputType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getClass(), condition, trueValue, falseValue, outputType);
    }
}
