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
package io.trino.sql.planner.optimizations.ctereuse;

import io.trino.sql.dialect.trino.operation.TrinoOperation;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.planner.optimizations.ctereuse.DynamicFilterUtils.DynamicFilterExtractionResult;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * This comparator is for testing purpose only. It ignores derived attributes when comparing operations.
 * <p>
 * Currently, CTE reuse and its utils often do not respect child attributes when transforming operations.
 * Instead of passing the actual child attributes, they pass empty attributes.
 * Hence, the attributes cannot be properly derived for the transformed operation.
 * It is not an issue as CTE reuse and its utils do not use derived attributes for their logic.
 * <p>
 * When we implement the Exploratory Optimizer framework, it will handle the proper attribute
 * propagation. We will migrate CTE reuse and related utils to the new framework.
 */
public class ComparatorIgnoringDerivedAttributes
{
    private ComparatorIgnoringDerivedAttributes() {}

    public static Comparator<Block> blockComparatorIgnoringDerivedAttributes()
    {
        return (actual, expected) -> {
            requireNonNull(actual, "actual is null");
            requireNonNull(expected, "expected is null");
            return blocksEqualIgnoringDerivedAttributes(actual, expected) ? 0 : -1;
        };
    }

    public static Comparator<Operation> operationComparatorIgnoringDerivedAttributes()
    {
        return (actual, expected) -> {
            requireNonNull(actual, "actual is null");
            requireNonNull(expected, "expected is null");
            return operationsEqualIgnoringDerivedAttributes(actual, expected) ? 0 : -1;
        };
    }

    public static Comparator<DynamicFilterExtractionResult> extractionResultComparatorIgnoringDerivedAttributes()
    {
        return (actual, expected) -> {
            requireNonNull(actual, "actual is null");
            requireNonNull(expected, "expected is null");
            return blocksEqualIgnoringDerivedAttributes(actual.dynamicPredicate(), expected.dynamicPredicate()) &&
                    blocksEqualIgnoringDerivedAttributes(actual.staticPredicate(), expected.staticPredicate()) ? 0 : -1;
        };
    }

    private static boolean blocksEqualIgnoringDerivedAttributes(List<Block> leftBlocks, List<Block> rightBlocks)
    {
        if (leftBlocks.size() != rightBlocks.size()) {
            return false;
        }
        for (int i = 0; i < leftBlocks.size(); i++) {
            if (!blocksEqualIgnoringDerivedAttributes(leftBlocks.get(i), rightBlocks.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean blocksEqualIgnoringDerivedAttributes(Block left, Block right)
    {
        return Objects.equals(left.name(), right.name()) &&
                Objects.equals(left.parameters(), right.parameters()) &&
                operationsEqualIgnoringDerivedAttributes(left.operations(), right.operations());
    }

    private static boolean operationsEqualIgnoringDerivedAttributes(List<Operation> leftOperations, List<Operation> rightOperations)
    {
        if (leftOperations.size() != rightOperations.size()) {
            return false;
        }
        for (int i = 0; i < leftOperations.size(); i++) {
            if (!operationsEqualIgnoringDerivedAttributes(leftOperations.get(i), rightOperations.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean operationsEqualIgnoringDerivedAttributes(Operation left, Operation right)
    {
        return Objects.equals(left.dialect(), right.dialect()) &&
                Objects.equals(left.name(), right.name()) &&
                Objects.equals(left.result(), right.result()) &&
                Objects.equals(left.arguments(), right.arguments()) &&
                blocksEqualIgnoringDerivedAttributes(
                        left.regions().stream().map(Region::getOnlyBlock).collect(toImmutableList()),
                        right.regions().stream().map(Region::getOnlyBlock).collect(toImmutableList())) &&
                Objects.equals(((TrinoOperation) left).operationAttributes(), ((TrinoOperation) right).operationAttributes());
    }
}
