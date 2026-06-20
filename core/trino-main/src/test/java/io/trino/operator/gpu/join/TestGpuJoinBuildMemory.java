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

import ai.rapids.cudf.ast.AstExpression;
import ai.rapids.cudf.ast.BinaryOperation;
import ai.rapids.cudf.ast.BinaryOperator;
import ai.rapids.cudf.ast.ColumnReference;
import ai.rapids.cudf.ast.Literal;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.operator.gpu.GpuOperation;
import io.trino.operator.gpu.GpuOperation.Blocked;
import io.trino.operator.gpu.GpuOperation.Data;
import io.trino.operator.gpu.GpuOperation.Finished;
import io.trino.operator.gpu.GpuOperation.Yielded;
import io.trino.operator.gpu.SettableGpuOperation;
import io.trino.operator.gpu.TestingGpuOperationContext;
import io.trino.operator.gpu.memory.GpuDeviceMemoryUsageValidation;
import io.trino.spi.Page;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.operator.gpu.GpuTestUtils.copyToDevice;
import static io.trino.operator.gpu.GpuTestUtils.createPages;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.gen.TestColumnarFilters.NullsProvider.NO_NULLS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Isolated // for memory accounting
@Execution(SAME_THREAD) // for memory accounting
class TestGpuJoinBuildMemory
{
    private static final int ROWS_PER_PAGE = 1_234_567;
    private static final int MULTIPLE_PAGES = 4;

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @Test
    void testBigintKeyNoOutput()
    {
        testBuild(List.of(BIGINT), new int[] {0}, new int[] {}, Optional.empty());
    }

    @Test
    void testVarcharKeyNoOutput()
    {
        testBuild(List.of(VARCHAR), new int[] {0}, new int[] {}, Optional.empty());
    }

    @Test
    void testCompositeKeyNoOutput()
    {
        testBuild(List.of(BIGINT, VARCHAR, VARCHAR), new int[] {0, 1, 2}, new int[] {}, Optional.empty());
    }

    @Test
    void testCompositeKeyWithOutput()
    {
        // key: BIGINT, VARCHAR, VARCHAR; output: VARCHAR, VARCHAR, DECIMAL(18,3)
        testBuild(
                List.of(BIGINT, VARCHAR, VARCHAR, VARCHAR, VARCHAR, createDecimalType(18, 3)),
                new int[] {0, 1, 2},
                new int[] {3, 4, 5},
                Optional.empty());
    }

    @Test
    void testCompositeKeyWithFilter()
    {
        testBuild(
                List.of(BIGINT, VARCHAR, VARCHAR),
                new int[] {0, 1, 2},
                new int[] {},
                // inequality filter on the BIGINT key (col 0 of build side)
                Optional.of(new BinaryOperation(BinaryOperator.LESS, new ColumnReference(0), Literal.ofLong(42))));
    }

    @Test
    void testCompositeKeyWithOutputAndFilter()
    {
        testBuild(
                List.of(BIGINT, VARCHAR, VARCHAR, VARCHAR, VARCHAR, createDecimalType(18, 3)),
                new int[] {0, 1, 2},
                new int[] {3, 4, 5},
                Optional.of(new BinaryOperation(BinaryOperator.LESS, new ColumnReference(0), Literal.ofLong(42))));
    }

    private void testBuild(List<Type> buildTypes, int[] keyChannels, int[] outputChannels, Optional<AstExpression> filter)
    {
        testBuild(buildTypes, keyChannels, outputChannels, filter, 1);
        testBuild(buildTypes, keyChannels, outputChannels, filter, MULTIPLE_PAGES);
    }

    private void testBuild(List<Type> buildTypes, int[] keyChannels, int[] outputChannels, Optional<AstExpression> filter, int pageCount)
    {
        TestingGpuOperationContext context = new TestingGpuOperationContext();
        try (GpuDeviceMemoryUsageValidation memoryValidation = GpuDeviceMemoryUsageValidation.createAndRegister(context, 1024 * 1024)) {
            Iterator<Page> inputPages = createPages(buildTypes, NO_NULLS, pageCount, ROWS_PER_PAGE, true);

            GpuJoinBridgeManager manager = new GpuJoinBridgeManager();
            GpuJoinBuild.Factory factory = new GpuJoinBuild.Factory(manager, keyChannels, outputChannels, filter, Optional.empty());
            try (var source = new SettableGpuOperation();
                    GpuOperation build = factory.create(context, source)) {
                // Drive until build published
                ListenableFuture<Void> blockedFuture = null;
                while (blockedFuture == null) {
                    switch (build.execute()) {
                        case Yielded() -> {
                            if (!source.hasPending()) {
                                memoryValidation.withoutValidation(() -> {
                                    if (inputPages.hasNext()) {
                                        GpuPage next = getOnlyElement(copyToDevice(List.of(inputPages.next()), buildTypes));
                                        source.setPending(context.taskMemoryContext().allocate(getClass().getSimpleName(), next.retainedMemory()), next);
                                    }
                                    else {
                                        source.noMoreInput();
                                    }
                                });
                            }
                        }
                        case Blocked(var future) -> {
                            checkState(source.isExhausted(), "Input not exhausted yet");
                            blockedFuture = future;
                        }
                        case Data _ -> throw new IllegalStateException("Build should not emit data");
                        case Finished() -> throw new IllegalStateException("Unexpected finish");
                    }
                }

                // Simulate all probes done
                verify(!blockedFuture.isDone(), "blockedFuture done too early");
                manager.probeOperatorFactoryClosed();
                verify(blockedFuture.isDone(), "blockedFuture still not done");

                // Advance build to Finished
                boolean finished = false;
                while (!finished) {
                    switch (build.execute()) {
                        case Yielded() -> {}
                        case Data _ -> throw new IllegalStateException("Build should not emit data");
                        case Blocked _ -> throw new IllegalStateException("Unexpected blocked");
                        case Finished() -> finished = true;
                    }
                }
            }
        }
    }
}
