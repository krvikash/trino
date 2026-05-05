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
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.operator.ReferenceCount;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Own;
import jakarta.annotation.Nullable;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

/**
 * GPU-resident hash table for a single join. Built once by {@link GpuJoinBuild} drivers
 * (via {@link GpuJoinBridgeManager}) and shared by all probe drivers participating in the
 * same join. Holds the cuDF {@link HashJoin} keyed on the build-side join columns and the
 * build-side payload {@link Table} used to gather build output rows.
 * <p>
 * When the build side has no rows {@link #hashJoin()} returns null; probe operators emit
 * no matches for INNER joins and fill build columns with nulls for LEFT joins. The
 * payload table is null when the join produces no build-side output columns.
 * <p>
 * Lifetime is reference-counted via {@link #retain()} and {@link #release()}.
 */
public final class GpuJoinBridge
{
    private final @Nullable @Own HashJoin hashJoin;
    private final @Nullable @Own Table buildTable;
    private final ReferenceCount refCount;

    GpuJoinBridge(@Nullable @Own Table buildTable, @Nullable @Own HashJoin hashJoin, int initialRefCount)
    {
        this.hashJoin = hashJoin;
        this.buildTable = buildTable;
        this.refCount = new ReferenceCount(initialRefCount);
        this.refCount.getFreeFuture().addListener(this::free, directExecutor());
    }

    private void free()
    {
        if (buildTable != null) {
            buildTable.close();
        }
        if (hashJoin != null) {
            hashJoin.close();
        }
    }

    /**
     * @return the cuDF hash join, or null when the build side produced zero rows.
     */
    public @Nullable @Borrow HashJoin hashJoin()
    {
        return hashJoin;
    }

    /**
     * @return the build-side table to gather from, or null when the join has no
     * build-side output columns or the build side produced zero rows.
     */
    public @Nullable @Borrow Table buildTable()
    {
        return buildTable;
    }

    public void retain()
    {
        refCount.retain();
    }

    public void release()
    {
        refCount.release();
    }

    /**
     * @return future that completes when the bridge is no longer referenced by any probe
     * operator and the manager's seed reference has been released.
     */
    public ListenableFuture<Void> getFreeFuture()
    {
        return refCount.getFreeFuture();
    }
}
