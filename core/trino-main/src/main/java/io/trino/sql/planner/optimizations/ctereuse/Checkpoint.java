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
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.sql.dialect.trino.ProgramBuilder;
import io.trino.sql.dialect.trino.operation.Exchange;
import io.trino.sql.dialect.trino.operation.TableScan;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.dialect.trino.Attributes.EXCHANGE_SCOPE;
import static io.trino.sql.dialect.trino.Attributes.ExchangeScope.REMOTE;
import static java.util.Objects.requireNonNull;

public sealed interface Checkpoint
        permits Checkpoint.BottomCheckpoint, Checkpoint.IntermediateCheckpoint
{
    int branchesCount();

    CteReuse.UnifiedStates extractSubgroup(
            List<Integer> subgroupIndexes,
            Map<Operation, Operation> usesMap,
            ProgramBuilder.ValueNameAllocator nameAllocator,
            Map<Value, Operation> newOperations,
            Session session,
            Metadata metadata);

    Checkpoint extractSubgroupCheckpoint(List<Integer> subgroupIndexes);

    record BottomCheckpoint(List<TableScan> tableScans)
            implements Checkpoint
    {
        public BottomCheckpoint
        {
            requireNonNull(tableScans, "tableScans is null");
            tableScans = ImmutableList.copyOf(tableScans);
        }

        @Override
        public int branchesCount()
        {
            return tableScans.size();
        }

        @Override
        public CteReuse.UnifiedStates extractSubgroup(
                List<Integer> subgroupIndexes,
                Map<Operation, Operation> usesMap,
                ProgramBuilder.ValueNameAllocator nameAllocator,
                Map<Value, Operation> newOperations,
                Session session,
                Metadata metadata)
        {
            checkArgument(subgroupIndexes.size() > 1, "subgroup must have at least 2 elements");

            List<TableScan> subgroupScans = subgroupIndexes.stream()
                    .map(tableScans::get)
                    .collect(toImmutableList());

            List<CteReuse.UnifiedGroup> unifiedGroups = CteReuse.unifyTableSubgroups(subgroupScans, session, metadata);
            checkState(unifiedGroups.size() == 1 && getOnlyElement(unifiedGroups).tableScans().size() == subgroupScans.size(), "failed to unify a subgroup");

            return CteReuse.initializeTraversalForGroup(getOnlyElement(unifiedGroups), metadata, usesMap, nameAllocator, newOperations);
        }

        @Override
        public Checkpoint extractSubgroupCheckpoint(List<Integer> subgroupIndexes)
        {
            checkArgument(subgroupIndexes.size() > 1, "subgroup must have at least 2 elements");

            List<TableScan> subgroupScans = subgroupIndexes.stream()
                    .map(tableScans::get)
                    .collect(toImmutableList());

            return new BottomCheckpoint(subgroupScans);
        }
    }

    record IntermediateCheckpoint(CteReuse.UnifiedStates unifiedStates)
            implements Checkpoint
    {
        public IntermediateCheckpoint
        {
            requireNonNull(unifiedStates, "unifiedStates is null");
            checkArgument(
                    unifiedStates.unifiedOperation() instanceof Exchange exchange && EXCHANGE_SCOPE.getAttribute(exchange.attributes()).equals(REMOTE),
                    "intermediate checkpoint must be a remote exchange");
        }

        @Override
        public int branchesCount()
        {
            return unifiedStates.residualStates().size();
        }

        @Override
        public CteReuse.UnifiedStates extractSubgroup(
                List<Integer> subgroupIndexes,
                Map<Operation, Operation> usesMap,
                ProgramBuilder.ValueNameAllocator nameAllocator,
                Map<Value, Operation> newOperations,
                Session session,
                Metadata metadata)
        {
            checkArgument(subgroupIndexes.size() > 1, "subgroup must have at least 2 elements");

            List<CteReuse.TraversalState> subgroupStates = subgroupIndexes.stream()
                    .map(unifiedStates.residualStates()::get)
                    .collect(toImmutableList());

            return new CteReuse.UnifiedStates(unifiedStates.unifiedOperation(), subgroupStates);
        }

        @Override
        public Checkpoint extractSubgroupCheckpoint(List<Integer> subgroupIndexes)
        {
            checkArgument(subgroupIndexes.size() > 1, "subgroup must have at least 2 elements");

            List<CteReuse.TraversalState> subgroupStates = subgroupIndexes.stream()
                    .map(unifiedStates.residualStates()::get)
                    .collect(toImmutableList());

            return new IntermediateCheckpoint(new CteReuse.UnifiedStates(unifiedStates.unifiedOperation(), subgroupStates));
        }
    }
}
