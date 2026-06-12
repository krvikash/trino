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

import com.google.common.collect.ImmutableList;
import io.trino.sql.planner.exploratory.physical.CostComparison;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;

final class CostOrdering
{
    private CostOrdering() {}

    /**
     * Returns a deterministic linear extension of the supplied cost partial order.
     * When multiple elements have no remaining better predecessor, their positions
     * in the input list determine their order.
     */
    static <T> List<T> stableLinearExtension(
            List<T> elements,
            BiFunction<T, T, CostComparison> comparison)
    {
        List<List<Integer>> successors = new ArrayList<>(elements.size());
        for (int index = 0; index < elements.size(); index++) {
            successors.add(new ArrayList<>());
        }
        int[] predecessorCounts = new int[elements.size()];

        for (int left = 0; left < elements.size(); left++) {
            for (int right = left + 1; right < elements.size(); right++) {
                switch (comparison.apply(elements.get(left), elements.get(right))) {
                    case BETTER -> addEdge(left, right, successors, predecessorCounts);
                    case WORSE -> addEdge(right, left, successors, predecessorCounts);
                    case EQUAL, INCOMPARABLE -> {}
                }
            }
        }

        PriorityQueue<Integer> available = new PriorityQueue<>();
        for (int index = 0; index < predecessorCounts.length; index++) {
            if (predecessorCounts[index] == 0) {
                available.add(index);
            }
        }

        ImmutableList.Builder<T> result = ImmutableList.builderWithExpectedSize(elements.size());
        int resultSize = 0;
        while (!available.isEmpty()) {
            int index = available.remove();
            result.add(elements.get(index));
            resultSize++;

            for (int successor : successors.get(index)) {
                predecessorCounts[successor]--;
                if (predecessorCounts[successor] == 0) {
                    available.add(successor);
                }
            }
        }

        checkArgument(resultSize == elements.size(), "cost comparison contains a cycle");
        return result.build();
    }

    private static void addEdge(
            int predecessor,
            int successor,
            List<List<Integer>> successors,
            int[] predecessorCounts)
    {
        successors.get(predecessor).add(successor);
        predecessorCounts[successor]++;
    }
}
