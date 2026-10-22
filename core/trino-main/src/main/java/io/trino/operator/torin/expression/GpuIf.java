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
package io.trino.operator.torin.expression;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.Scalar;
import ai.rapids.cudf.Table;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.torin.ClosingOnce;
import io.trino.spi.torin.borrow.Borrow;
import io.trino.spi.torin.borrow.Move;
import io.trino.spi.torin.borrow.Own;
import io.trino.spi.type.Type;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;

import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;

import static com.google.common.collect.Lists.transform;
import static io.trino.operator.torin.expression.CudfUtils.allTrue;
import static io.trino.operator.torin.expression.CudfUtils.anyTrue;
import static io.trino.operator.torin.expression.Fallibility.FALLIBLE_EXCEPT_ALL_NULLS;
import static io.trino.operator.torin.expression.Fallibility.best;
import static io.trino.operator.torin.expression.Fallibility.worst;
import static io.trino.operator.torin.expression.GpuExpressions.allNullOnNullInputs;
import static io.trino.operator.torin.expression.GpuExpressions.worstOf;
import static io.trino.plugin.base.torin.GpuUtils.closeColumns;
import static io.trino.spi.torin.GpuTypeConversion.nullScalar;
import static io.trino.spi.torin.GpuTypeConversion.toDType;
import static java.util.Objects.requireNonNull;

/**
 * {@code IF(condition, trueValue, falseValue)} with lazy evaluation semantics.
 *
 * <p>A branch must not raise a runtime error on a row it does not cover, because the CPU would
 * never have evaluated it there. How much that costs depends on the branch's
 * {@link Fallibility}: an {@link Fallibility#INFALLIBLE} branch can be evaluated over every row
 * and the two results combined, a {@link Fallibility#FALLIBLE_EXCEPT_ALL_NULLS} one over every row
 * as long as the rows it does not cover reach it with all input and bound columns NULL, and only a
 * {@link Fallibility#FALLIBLE} one has to be evaluated over a filtered copy of the input and
 * scattered back.
 *
 * <p>For example the true branch of {@code IF(b != 0, 10 / coalesce(b, 0), 42)} is
 * {@code FALLIBLE}: NULL-masking its input does not keep the divide-by-zero check away from it,
 * because {@code coalesce} resurrects the zero divisor from the NULL.
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

    /// How a branch can be evaluated so it doesn't fail on a row it does not cover.
    private enum BranchEvaluation
    {
        /// Over every row, as it is.
        EVERY_ROW,
        /// Over every row, with the input columns NULL wherever the branch is not selected.
        MASKED_INPUTS,
        /// Over a filtered copy of the input holding only the rows the branch covers.
        FILTERED,
    }

    /// The branch a row of the condition mask selects: TRUE rows select the true branch.
    private enum Branch
    {
        TRUE,
        FALSE,
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
            BranchEvaluation trueEvaluation = branchEvaluation(trueValue, inputColumns, bindings);
            BranchEvaluation falseEvaluation = branchEvaluation(falseValue, inputColumns, bindings);
            if (trueEvaluation != BranchEvaluation.FILTERED && falseEvaluation != BranchEvaluation.FILTERED) {
                try (ColumnVector trueResult = evaluateEveryRow(trueValue, trueEvaluation, conditionMask.borrow(), Branch.TRUE, positionCount, inputColumns, bindings);
                        ColumnVector falseResult = evaluateEveryRow(falseValue, falseEvaluation, conditionMask.borrow(), Branch.FALSE, positionCount, inputColumns, bindings)) {
                    return conditionMask.borrow().ifElse(trueResult, falseResult);
                }
            }
            try (ColumnVector notConditionMask = conditionMask.borrow().not();
                    Scalar zero = Scalar.fromInt(0);
                    ColumnVector sequence = ColumnVector.sequence(zero, positionCount);
                    Table sequenceTable = new Table(sequence)) {
                if (falseEvaluation != BranchEvaluation.FILTERED) {
                    try (ColumnVector falseResult = evaluateEveryRow(falseValue, falseEvaluation, conditionMask.borrow(), Branch.FALSE, positionCount, inputColumns, bindings);
                            ColumnVector trueResult = filterAndEvaluate(trueValue, conditionMask.borrow(), inputColumns, bindings);
                            Table trueIndices = sequenceTable.filter(conditionMask.borrow())) {
                        return scatterInto(trueResult, trueIndices.getColumn(0), falseResult);
                    }
                }
                if (trueEvaluation != BranchEvaluation.FILTERED) {
                    try (ColumnVector trueResult = evaluateEveryRow(trueValue, trueEvaluation, conditionMask.borrow(), Branch.TRUE, positionCount, inputColumns, bindings);
                            ColumnVector falseResult = filterAndEvaluate(falseValue, notConditionMask, inputColumns, bindings);
                            Table falseIndices = sequenceTable.filter(notConditionMask)) {
                        return scatterInto(falseResult, falseIndices.getColumn(0), trueResult);
                    }
                }
                try (ColumnVector trueResult = filterAndEvaluate(trueValue, conditionMask.borrow(), inputColumns, bindings);
                        Table trueIndices = sequenceTable.filter(conditionMask.borrow())) {
                    conditionMask.close();
                    try (ColumnVector falseResult = filterAndEvaluate(falseValue, notConditionMask, inputColumns, bindings);
                            Table falseIndices = sequenceTable.filter(notConditionMask)) {
                        return scatterCombine(outputType, positionCount, trueIndices.getColumn(0), falseIndices.getColumn(0), trueResult, falseResult);
                    }
                }
            }
        }
    }

    private BranchEvaluation branchEvaluation(GpuExpression branch, List<@Borrow ColumnVector> inputColumns, Bindings bindings)
    {
        if (toDType(outputType).orElseThrow().isNestedType()) {
            // cuDF ifElse does not combine nested columns, while scatter does
            return BranchEvaluation.FILTERED;
        }
        return switch (branch.fallibility()) {
            case INFALLIBLE -> BranchEvaluation.EVERY_ROW;
            case FALLIBLE_EXCEPT_ALL_NULLS -> {
                List<Entry<Integer, @Borrow ColumnVector>> allBindings = bindings.allBindings();
                if (inputColumns.isEmpty() && allBindings.isEmpty()) {
                    // Without input columns or bindings every row is vacuously all-NULL.
                    yield BranchEvaluation.EVERY_ROW;
                }
                if (inputColumns.stream().anyMatch(column -> column.getType().isNestedType()) || allBindings.stream().anyMatch(binding -> binding.getValue().getType().isNestedType())) {
                    // cuDF ifElse cannot replace the values of a nested column with NULL
                    yield BranchEvaluation.FILTERED;
                }
                yield BranchEvaluation.MASKED_INPUTS;
            }
            case FALLIBLE -> BranchEvaluation.FILTERED;
        };
    }

    /// Evaluates `branch` over every row, hiding the rows it does not cover behind NULL input columns
    /// when it needs that not to fail on them. `side` tells which rows of `conditionMask` `branch` covers.
    @SuppressModernizer // branchEvaluation excludes nested input columns from MASKED_INPUTS.
    private static @Move ColumnVector evaluateEveryRow(
            GpuExpression branch,
            BranchEvaluation evaluation,
            @Borrow ColumnVector conditionMask,
            Branch side,
            int positionCount,
            List<@Borrow ColumnVector> inputColumns,
            Bindings bindings)
    {
        if (evaluation == BranchEvaluation.EVERY_ROW) {
            return branch.evaluate(positionCount, inputColumns, bindings);
        }
        List<Entry<Integer, @Borrow ColumnVector>> allBindings = bindings.allBindings();
        List<@Borrow ColumnVector> allColumns = ImmutableList.<@Borrow ColumnVector>builder()
                .addAll(inputColumns)
                .addAll(transform(allBindings, Entry::getValue))
                .build();
        @Own ColumnVector[] maskedColumns = new ColumnVector[allColumns.size()];
        try {
            for (int i = 0; i < allColumns.size(); i++) {
                @Borrow ColumnVector column = allColumns.get(i);
                try (Scalar nullValue = Scalar.fromNull(column.getType())) {
                    // ifElse keeps the column's own NULLs, so this only ever adds NULLs
                    maskedColumns[i] = switch (side) {
                        case TRUE -> conditionMask.ifElse(column, nullValue);
                        case FALSE -> conditionMask.ifElse(nullValue, column);
                    };
                }
            }
            Bindings maskedBindings = new Bindings();
            for (int i = 0; i < allBindings.size(); i++) {
                maskedBindings.put(allBindings.get(i).getKey(), maskedColumns[inputColumns.size() + i]);
            }
            return branch.evaluate(positionCount, ImmutableList.copyOf(maskedColumns).subList(0, inputColumns.size()), maskedBindings);
        }
        finally {
            closeColumns(maskedColumns);
        }
    }

    /// Overlays `source` onto `target`: the positions listed in `indices` take their value from
    /// `source`, every other position keeps the one it has in `target`.
    private static @Move ColumnVector scatterInto(
            @Borrow ColumnVector source,
            @Borrow ColumnVector indices,
            @Borrow ColumnVector target)
    {
        try (Table sourceTable = new Table(source);
                Table targetTable = new Table(target);
                Table merged = sourceTable.scatter(indices, targetTable)) {
            return merged.getColumn(0).incRefCount();
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
