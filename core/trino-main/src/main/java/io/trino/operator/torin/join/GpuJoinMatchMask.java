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
package io.trino.operator.torin.join;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.ColumnView;
import ai.rapids.cudf.Cuda;
import ai.rapids.cudf.Scalar;
import ai.rapids.cudf.Table;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.operator.torin.CudaStreams;
import io.trino.plugin.base.torin.ClosingRef;
import io.trino.spi.torin.borrow.Borrow;
import io.trino.spi.torin.borrow.Move;
import io.trino.spi.torin.borrow.Own;

@ThreadSafe
public final class GpuJoinMatchMask
        implements AutoCloseable
{
    private final int buildRowCount;
    @GuardedBy("this")
    private final @Own ClosingRef<ColumnVector> buildRowMatched;

    public static @Move GpuJoinMatchMask allUnmatched(int buildRowCount)
    {
        try (Scalar unmatched = Scalar.fromBool(false)) {
            return new GpuJoinMatchMask(buildRowCount, ColumnVector.fromScalar(unmatched, buildRowCount));
        }
    }

    private GpuJoinMatchMask(int buildRowCount, @Move ColumnVector buildRowMatched)
    {
        this.buildRowCount = buildRowCount;
        this.buildRowMatched = ClosingRef.own(buildRowMatched);
    }

    public synchronized void markMatched(@Borrow ColumnView distinctBuildIndices)
    {
        try (Scalar matched = Scalar.fromBool(true);
                Table target = new Table(buildRowMatched.borrow());
                Table scattered = Table.scatter(new Scalar[] {matched}, distinctBuildIndices, target)) {
            buildRowMatched.replace(scattered.getColumn(0).incRefCount());
        }
        // the mask will be observable by other threads
        CudaStreams.sync(Cuda.DEFAULT_STREAM);
    }

    public synchronized @Move ColumnVector unmatchedBuildIndices()
    {
        try (Scalar zero = Scalar.fromInt(0);
                ColumnVector rowNumbers = ColumnVector.sequence(zero, buildRowCount);
                ColumnVector unmatchedMask = buildRowMatched.borrow().not();
                Table rows = new Table(rowNumbers);
                Table unmatched = rows.filter(unmatchedMask)) {
            return unmatched.getColumn(0).incRefCount();
        }
    }

    @Override
    public synchronized void close()
    {
        buildRowMatched.close();
    }
}
