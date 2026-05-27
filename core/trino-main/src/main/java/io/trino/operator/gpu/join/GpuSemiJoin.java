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

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.Scalar;
import ai.rapids.cudf.Table;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.operator.gpu.GpuOperation;
import io.trino.operator.gpu.join.GpuSemiJoinSetSupplier.GpuSemiJoinSet;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.plugin.base.gpu.UncheckedCloser;
import io.trino.spi.gpu.Column;
import io.trino.spi.gpu.Column.DeviceMemory;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.concurrent.MoreFutures.getDone;
import static io.trino.operator.gpu.GpuUtils.closeColumns;
import static java.util.Objects.requireNonNull;

public final class GpuSemiJoin
        implements GpuOperation
{
    public static final class Factory
            implements GpuOperation.Factory
    {
        private final GpuSemiJoinSetSupplier setSupplier;
        private final int probeKeyChannel;

        private boolean closed;

        public Factory(GpuSemiJoinSetSupplier setSupplier, int probeKeyChannel)
        {
            this.setSupplier = requireNonNull(setSupplier, "setSupplier is null");
            this.probeKeyChannel = probeKeyChannel;
        }

        @Override
        public Factory duplicate()
        {
            checkState(!closed, "Already closed");
            setSupplier.probeOperatorFactoryDuplicated();
            return new Factory(setSupplier, probeKeyChannel);
        }

        @Override
        public GpuOperation create(GpuOperation source)
        {
            return new GpuSemiJoin(source, setSupplier, probeKeyChannel);
        }

        @Override
        public void noMoreOperators()
        {
            checkState(!closed, "Already closed");
            closed = true;
            setSupplier.probeOperatorFactoryClosed();
        }
    }

    private final UncheckedCloser closer = UncheckedCloser.create();
    private final GpuOperation source;
    private final ListenableFuture<GpuSemiJoinSet> setFuture;
    private final int probeKeyChannel;

    private GpuSemiJoin(GpuOperation source, GpuSemiJoinSetSupplier setSupplier, int probeKeyChannel)
    {
        this.source = requireNonNull(source, "source is null");
        this.setFuture = setSupplier.getSetFuture();
        this.probeKeyChannel = probeKeyChannel;

        closer.register(source);
        closer.register(setSupplier::probeOperatorClosed);
    }

    @Override
    public @Move Result execute()
    {
        if (!setFuture.isDone()) {
            return new Blocked(asVoid(setFuture));
        }
        GpuSemiJoinSet set = getDone(setFuture);

        @Own Result sourceResult = source.execute();
        return switch (sourceResult) {
            case Blocked blocked -> blocked;
            case Yielded yielded -> yielded;
            case Finished finished -> finished;
            case Data(GpuPage page) -> {
                try (page) {
                    yield new Data(processProbePage(page, set));
                }
            }
        };
    }

    private @Move GpuPage processProbePage(@Borrow GpuPage probePage, GpuSemiJoinSet set)
    {
        @Own Column[] outputColumns = new Column[probePage.columnCount() + 1];
        try {
            for (int i = 0; i < probePage.columnCount(); i++) {
                outputColumns[i] = probePage.column(i).incRefCount();
            }
            outputColumns[probePage.columnCount()] = new DeviceMemory(computeMembership(probePage, set));
            return new GpuPage(probePage.positionCount(), outputColumns);
        }
        finally {
            closeColumns(outputColumns);
        }
    }

    private @Move ColumnVector computeMembership(@Borrow GpuPage probePage, GpuSemiJoinSet set)
    {
        Optional<@Borrow Table> buildKeys = set.buildKeys();
        if (buildKeys.isEmpty()) {
            try (Scalar falseScalar = Scalar.fromBool(false)) {
                return ColumnVector.fromScalar(falseScalar, probePage.positionCount());
            }
        }

        @Borrow ColumnVector probeKey = ((DeviceMemory) probePage.column(probeKeyChannel)).columnVector();
        @Borrow ColumnVector buildKey = buildKeys.get().getColumn(0);

        try (ClosingRef<ColumnVector> matched = ClosingRef.own(probeKey.contains(buildKey))) {
            if (!set.buildHasNull()) {
                return matched.take();
            }
            // build has nulls: "no match" must become NULL per three-valued IN semantics.
            try (Scalar nullScalar = Scalar.fromNull(DType.BOOL8);
                    Scalar falseScalar = Scalar.fromBool(false);
                    ColumnVector isFalse = matched.borrow().equalTo(falseScalar)) {
                return isFalse.ifElse(nullScalar, matched.borrow());
            }
        }
    }

    @Override
    public void close()
    {
        closer.close();
    }
}
