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
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;

import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMultiset.toImmutableMultiset;

public class BranchesToCheckpointsMapping
{
    private final List<CheckpointReferences> mapping;

    private BranchesToCheckpointsMapping(List<CheckpointReferences> mapping)
    {
        this.mapping = ImmutableList.copyOf(mapping);
    }

    public static BranchesToCheckpointsMapping identityBranchToCheckpoint(int branchesCount)
    {
        return new BranchesToCheckpointsMapping(IntStream.range(0, branchesCount)
                .boxed()
                .map(List::of)
                .map(List::of)
                .map(CheckpointReferences::new)
                .collect(toImmutableList()));
    }

    public static BranchesToCheckpointsMapping fromBranchMappings(List<CheckpointReferences> mappingForBranches)
    {
        return new BranchesToCheckpointsMapping(mappingForBranches);
    }

    public CheckpointReferences getMappingForBranch(int branch)
    {
        return mapping.get(branch);
    }

    public void verifyMapping(int branchesCount, List<Checkpoint> checkpoints)
    {
        checkArgument(branchesCount == mapping.size(), "branches and mapping mismatch");
        checkArgument(!checkpoints.isEmpty(), "checkpoints is empty");
        checkArgument(
                mapping.stream()
                        .mapToInt(CheckpointReferences::getCheckpointsCount)
                        .allMatch(size -> size == checkpoints.size()),
                "mapping and checkpoints mismatch");
        for (int i = 0; i < checkpoints.size(); i++) {
            Multiset<Integer> checkpointStates = IntStream.range(0, checkpoints.get(i).branchesCount())
                    .boxed()
                    .collect(toImmutableMultiset());
            ImmutableList.Builder<Integer> referencedCheckpointStatesSizes = ImmutableList.builder();
            ImmutableMultiset.Builder<Integer> referencedCheckpointStates = ImmutableMultiset.builder();
            for (CheckpointReferences checkpointReferences : mapping) {
                referencedCheckpointStatesSizes.add(checkpointReferences.getReferencesForCheckpoint(i).size());
                referencedCheckpointStates.addAll(checkpointReferences.getReferencesForCheckpoint(i));
            }
            checkArgument(referencedCheckpointStatesSizes.build().stream().distinct().count() == 1, "a checkpoint should have the same number of referenced states from all branches");
            checkArgument(checkpointStates.equals(referencedCheckpointStates.build()), "each checkpoint state should be referenced exactly once");
        }
    }

    public static class CheckpointReferences
    {
        private final List<List<Integer>> checkpointReferences;

        public CheckpointReferences(List<List<Integer>> checkpointReferences)
        {
            this.checkpointReferences = checkpointReferences.stream()
                    .map(List::copyOf)
                    .collect(toImmutableList());
        }

        public static CheckpointReferences concatenateCheckpointReferences(List<CheckpointReferences> componentReferences)
        {
            return new CheckpointReferences(componentReferences.stream()
                    .map(CheckpointReferences::getCheckpointReferences)
                    .flatMap(List::stream)
                    .collect(toImmutableList()));
        }

        public List<Integer> getReferencesForCheckpoint(int checkpoint)
        {
            return checkpointReferences.get(checkpoint);
        }

        public int getCheckpointsCount()
        {
            return checkpointReferences.size();
        }

        private List<List<Integer>> getCheckpointReferences()
        {
            return checkpointReferences;
        }
    }
}
