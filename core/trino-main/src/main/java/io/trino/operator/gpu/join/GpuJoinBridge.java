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
package io.trino.operator.gpu.join;

import ai.rapids.cudf.HashJoin;
import ai.rapids.cudf.Table;
import ai.rapids.cudf.ast.CompiledExpression;
import io.trino.spi.gpu.borrow.Borrow;
import jakarta.annotation.Nullable;

import static java.util.Objects.requireNonNull;

public sealed interface GpuJoinBridge
{
    @Nullable
    @Borrow
    Table buildOutputTable();

    record EmptyBuildSide()
            implements GpuJoinBridge
    {
        @Override
        public @Nullable @Borrow Table buildOutputTable()
        {
            return null;
        }
    }

    record HashJoinBridge(@Borrow HashJoin hashJoin, @Nullable @Borrow Table buildOutputTable)
            implements GpuJoinBridge
    {
        public HashJoinBridge
        {
            requireNonNull(hashJoin, "hashJoin is null");
        }
    }

    record FilteredHashJoinBridge(
            @Borrow Table buildSourceTable,
            @Borrow Table buildKeysTable,
            @Borrow CompiledExpression compiledFilter,
            @Nullable @Borrow Table buildOutputTable)
            implements GpuJoinBridge
    {
        public FilteredHashJoinBridge
        {
            requireNonNull(buildSourceTable, "buildSourceTable is null");
            requireNonNull(buildKeysTable, "buildKeysTable is null");
            requireNonNull(compiledFilter, "compiledFilter is null");
        }
    }
}
