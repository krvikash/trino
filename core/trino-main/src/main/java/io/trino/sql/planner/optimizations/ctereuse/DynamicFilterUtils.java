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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.predicate.NullableValue;
import io.trino.sql.DynamicFilters;
import io.trino.sql.dialect.trino.ProgramBuilder;
import io.trino.sql.dialect.trino.operation.Call;
import io.trino.sql.dialect.trino.operation.Constant;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.dialect.trino.Attributes.CONSTANT_RESULT;
import static io.trino.sql.dialect.trino.Attributes.RESOLVED_FUNCTION;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.conjunction;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.extractConjuncts;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.optimizeLogicalOperations;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.truePredicate;
import static java.util.Objects.requireNonNull;

public class DynamicFilterUtils
{
    private DynamicFilterUtils() {}

    /**
     * Extract dynamic filters from the predicate, combine them in a conjunction and return them as a separate block.
     * Also return the remaining conjunct of the predicate.
     * The resulting blocks have the same name and parameters as the input block.
     */
    public static DynamicFilterExtractionResult extractDynamicFilters(Block predicate, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        Block optimizedPredicate = optimizeLogicalOperations(predicate);
        List<Block> conjuncts = extractConjuncts(optimizedPredicate, nameAllocator);
        ImmutableList.Builder<Block> dynamicConjunctsBuilder = ImmutableList.builder();
        ImmutableList.Builder<Block> staticConjunctsBuilder = ImmutableList.builder();
        conjuncts.stream()
                .forEach(block -> {
                    if (isDynamicFilter(block)) {
                        dynamicConjunctsBuilder.add(block);
                    }
                    else {
                        checkState(!containsDynamicFilter(block), "unexpected dynamic filter");
                        staticConjunctsBuilder.add(block);
                    }
                });
        List<Block> dynamicConjuncts = dynamicConjunctsBuilder.build();
        Block dynamicPredicate = dynamicConjuncts.isEmpty() ? truePredicate(predicate.name(), predicate.parameters(), nameAllocator) : conjunction(dynamicConjuncts, nameAllocator);

        List<Block> staticConjuncts = staticConjunctsBuilder.build();
        Block staticPredicate = staticConjuncts.isEmpty() ? truePredicate(predicate.name(), predicate.parameters(), nameAllocator) : conjunction(staticConjuncts, nameAllocator);

        return new DynamicFilterExtractionResult(dynamicPredicate, staticPredicate);
    }

    /**
     * Split the predicate into dynamic and static parts. The dynamic part is the minimal conjunct with the dynamic filter function.
     * The resulting blocks have the same name and parameters as the input block.
     */
    public static DynamicFilterExtractionResult extractDynamicConjunct(Block predicate, ProgramBuilder.ValueNameAllocator nameAllocator)
    {
        Block optimizedPredicate = optimizeLogicalOperations(predicate);
        List<Block> conjuncts = extractConjuncts(optimizedPredicate, nameAllocator);
        ImmutableList.Builder<Block> dynamicConjunctsBuilder = ImmutableList.builder();
        ImmutableList.Builder<Block> staticConjunctsBuilder = ImmutableList.builder();
        conjuncts.stream()
                .forEach(block -> {
                    if (containsDynamicFilter(block)) {
                        dynamicConjunctsBuilder.add(block);
                    }
                    else {
                        staticConjunctsBuilder.add(block);
                    }
                });
        List<Block> dynamicConjuncts = dynamicConjunctsBuilder.build();
        Block dynamicPredicate = dynamicConjuncts.isEmpty() ? truePredicate(predicate.name(), predicate.parameters(), nameAllocator) : conjunction(dynamicConjuncts, nameAllocator);

        List<Block> staticConjuncts = staticConjunctsBuilder.build();
        Block staticPredicate = staticConjuncts.isEmpty() ? truePredicate(predicate.name(), predicate.parameters(), nameAllocator) : conjunction(staticConjuncts, nameAllocator);

        return new DynamicFilterExtractionResult(dynamicPredicate, staticPredicate);
    }

    public static boolean isDynamicFilter(Block block)
    {
        int size = block.operations().size();
        return size >= 7 &&
                block.operations().get(size - 6) instanceof Constant &&
                block.operations().get(size - 5) instanceof Constant &&
                block.operations().get(size - 4) instanceof Constant &&
                block.operations().get(size - 3) instanceof Constant &&
                isDynamicFilterFunction(block.operations().get(size - 2)) &&
                block.operations().get(size - 2).arguments().equals(ImmutableList.of(
                        block.operations().get(size - 7).result(),
                        block.operations().get(size - 6).result(),
                        block.operations().get(size - 5).result(),
                        block.operations().get(size - 4).result(),
                        block.operations().get(size - 3).result())) &&
                block.operations().get(size - 1) instanceof Return returnOperation &&
                returnOperation.argument().equals(block.operations().get(size - 2).result());
    }

    public static boolean isDynamicFilterFunction(Operation operation)
    {
        if (!(operation instanceof Call call)) {
            return false;
        }
        CatalogSchemaFunctionName functionName = RESOLVED_FUNCTION.getAttribute(call.attributes()).name();
        return functionName.equals(builtinFunctionName(DynamicFilters.Function.NAME)) || functionName.equals(builtinFunctionName(DynamicFilters.NullableFunction.NAME));
    }

    private static boolean containsDynamicFilter(Block block)
    {
        for (Operation operation : block.operations()) {
            if (isDynamicFilterFunction(operation)) {
                return true;
            }
            if (operation.regions().stream()
                    .map(Region::getOnlyBlock)
                    .anyMatch(DynamicFilterUtils::containsDynamicFilter)) {
                return true;
            }
        }
        return false;
    }

    public static String getDynamicFilterId(Block block)
    {
        checkArgument(isDynamicFilter(block), "expected dynamic filter");

        Constant idOperation = (Constant) block.operations().get(block.operations().size() - 5);
        NullableValue idAttribute = CONSTANT_RESULT.getAttribute(idOperation.attributes());
        checkArgument(idAttribute.getType().equals(VARCHAR), "expected dynamic filter id to be of varchar type");
        return ((Slice) idAttribute.getValue()).toStringUtf8();
    }

    public record DynamicFilterExtractionResult(Block dynamicPredicate, Block staticPredicate)
    {
        public DynamicFilterExtractionResult
        {
            requireNonNull(dynamicPredicate, "dynamicPredicate is null");
            requireNonNull(staticPredicate, "staticPredicate is null");
        }
    }
}
