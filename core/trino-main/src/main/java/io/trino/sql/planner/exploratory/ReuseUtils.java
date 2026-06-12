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
package io.trino.sql.planner.exploratory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.trino.sql.dialect.trino.operation.Query;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Operation.Result;
import io.trino.sql.newir.Program;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class ReuseUtils
{
    private ReuseUtils() {}

    /**
     * Find operations that are reused in the Program. Identify them by (block id, operation result),
     * where Block ids are assigned following the traversal order of the program.
     */
    public static Set<BlockAndValue> getReusedOperations(Program program)
    {
        return getReusedOperations(((Query) program.root()).query());
    }

    @VisibleForTesting
    static Set<BlockAndValue> getReusedOperations(Block block)
    {
        ImmutableMultiset.Builder<BlockAndValue> usedOperations = ImmutableMultiset.builder();
        getUsedOperations(block, new Memo.IdAllocator(), new HashMap<>(), usedOperations);

        return usedOperations.build().entrySet().stream()
                .filter(entry -> entry.getCount() > 1)
                .map(Multiset.Entry::getElement)
                .collect(toImmutableSet());
    }

    private static void getUsedOperations(Block block, Memo.IdAllocator blockIdAllocator, Map<Result, Integer> definingBlock, ImmutableMultiset.Builder<BlockAndValue> usedOperations)
    {
        int blockId = blockIdAllocator.newId();
        for (Operation operation : block.operations()) {
            for (Value argument : operation.arguments()) {
                if (argument instanceof Result result) {
                    // although the same result might occur in different blocks, only one definition can be visible at a certain site.
                    // we ensure identifying the right definition by using scope isolation of definingBlock map for each nested region.
                    Integer definingBlockId = definingBlock.get(result);
                    requireNonNull(definingBlockId, "invalid program: use of undeclared operation result: " + result.name());
                    usedOperations.add(new BlockAndValue(definingBlockId, result));
                }
            }
            for (Region region : operation.regions()) {
                // clone the definingBlock map for each nested region to ensure scope isolation
                getUsedOperations(region.getOnlyBlock(), blockIdAllocator, new HashMap<>(definingBlock), usedOperations);
            }
            Integer previousDefinition = definingBlock.put(operation.result(), blockId);
            checkArgument(previousDefinition == null, "invalid program: duplicate declaration: %s", operation.result().name());
        }
    }

    public record BlockAndValue(int blockId, Result result)
    {
        public BlockAndValue
        {
            requireNonNull(result, "result is null");
        }
    }
}
