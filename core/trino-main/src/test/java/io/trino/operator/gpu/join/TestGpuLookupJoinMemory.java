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
import ai.rapids.cudf.ast.TableReference;
import com.google.common.collect.ImmutableList;
import io.trino.operator.gpu.GpuOperation;
import io.trino.operator.gpu.GpuOperation.Blocked;
import io.trino.operator.gpu.GpuOperation.Data;
import io.trino.operator.gpu.GpuOperation.Finished;
import io.trino.operator.gpu.GpuOperation.Yielded;
import io.trino.operator.gpu.SettableGpuOperation;
import io.trino.operator.gpu.TestingGpuOperationContext;
import io.trino.operator.gpu.join.GpuLookupJoin.JoinType;
import io.trino.operator.gpu.memory.AllocatedMemory;
import io.trino.operator.gpu.memory.GpuDeviceMemoryUsageValidation;
import io.trino.plugin.base.gpu.UncheckedCloser;
import io.trino.spi.Page;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.operator.gpu.GpuTestUtils.copyToDevice;
import static io.trino.operator.gpu.GpuTestUtils.createPages;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.join.GpuLookupJoin.JoinType.LEFT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Isolated // for memory accounting
@Execution(SAME_THREAD) // for memory accounting
class TestGpuLookupJoinMemory
{
    private static final int BUILD_ROWS = 100_000;
    private static final int PROBE_ROWS_PER_PAGE = 200_000;

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @ParameterizedTest
    @EnumSource(JoinType.class)
    void testEmptyProbe(JoinType joinType)
    {
        testProbe(joinType, List.of(BIGINT), new int[] {0}, new int[] {}, new int[] {}, ImmutableList.of(), 1, 0);
    }

    @Test
    void testEmptyBuildLeftJoin()
    {
        testProbe(LEFT, List.of(BIGINT), new int[] {0}, new int[] {0}, new int[] {}, ImmutableList.of(BIGINT), 0, 1);
    }

    @ParameterizedTest
    @EnumSource(JoinType.class)
    void testBigintKey(JoinType joinType)
    {
        testProbe(joinType, List.of(BIGINT), new int[] {0}, new int[] {}, new int[] {}, ImmutableList.of(), 1, 1);
    }

    @ParameterizedTest
    @EnumSource(JoinType.class)
    void testVarcharKey(JoinType joinType)
    {
        testProbe(joinType, List.of(VARCHAR), new int[] {0}, new int[] {}, new int[] {}, ImmutableList.of(), 1, 1);
    }

    @ParameterizedTest
    @EnumSource(JoinType.class)
    void testCompositeKey(JoinType joinType)
    {
        testProbe(
                joinType,
                List.of(BIGINT, VARCHAR, VARCHAR, VARCHAR, createDecimalType(18, 3)),
                new int[] {0, 1, 2},
                new int[] {3, 4},
                new int[] {3, 4},
                List.of(VARCHAR, createDecimalType(18, 3)),
                1,
                1);
    }

    private void testProbe(
            JoinType joinType,
            List<Type> types,
            int[] keyChannels,
            int[] buildOutputChannels,
            int[] probeOutputChannels,
            List<Type> buildOutputTypes,
            int buildPageCount,
            int probePageCount)
    {
        for (NullsProvider nullsProvider : NullsProvider.values()) {
            // non-filtered
            testProbe(
                    joinType,
                    types,
                    nullsProvider,
                    keyChannels,
                    buildOutputChannels,
                    probeOutputChannels,
                    buildOutputTypes,
                    Optional.empty(),
                    buildPageCount,
                    probePageCount,
                    false);
            // filtered
            testProbe(
                    joinType,
                    types,
                    nullsProvider,
                    keyChannels,
                    buildOutputChannels,
                    probeOutputChannels,
                    buildOutputTypes,
                    Optional.of(new BinaryOperation(
                            BinaryOperator.NOT_EQUAL,
                            new ColumnReference(0, TableReference.LEFT),
                            new ColumnReference(0, TableReference.RIGHT))),
                    buildPageCount,
                    probePageCount,
                    true /* Larger margin because for filtered joins the output size (gather maps + gathered tables) is not estimated upfront. */);
        }
    }

    private void testProbe(
            JoinType joinType,
            List<Type> types,
            NullsProvider nullsProvider,
            int[] keyChannels,
            int[] buildOutputChannels,
            int[] probeOutputChannels,
            List<Type> buildOutputTypes,
            Optional<AstExpression> filter,
            int buildPageCount,
            int probePageCount,
            boolean largerMargin)
    {
        boolean filteredJoin = filter.isPresent();
        TestingGpuOperationContext context = new TestingGpuOperationContext();
        long marginBytes = (largerMargin ? 9L : 1L) * 1024 * 1024;
        try (GpuDeviceMemoryUsageValidation memoryValidation = GpuDeviceMemoryUsageValidation.createAndRegister(context, marginBytes)) {
            GpuJoinBridgeManager manager = new GpuJoinBridgeManager();
            GpuJoinBuild.Factory buildFactory = new GpuJoinBuild.Factory(manager, keyChannels, buildOutputChannels, filter, Optional.empty());

            try (var buildSource = new SettableGpuOperation();
                    GpuOperation build = buildFactory.create(context, buildSource)) {
                // Drive build to published — exempt from validation since we're testing probe
                memoryValidation.withoutValidation(() -> {
                    Iterator<Page> buildPages = createPages(types, nullsProvider, buildPageCount, BUILD_ROWS, false);
                    boolean buildBlocked = false;
                    while (!buildBlocked) {
                        switch (build.execute()) {
                            case Yielded() -> {
                                if (!buildSource.hasPending()) {
                                    if (buildPages.hasNext()) {
                                        GpuPage next = getOnlyElement(copyToDevice(List.of(buildPages.next()), types));
                                        buildSource.setPending(context.taskMemoryContext().allocate("build", next.retainedMemory()), next);
                                    }
                                    else {
                                        buildSource.noMoreInput();
                                    }
                                }
                            }
                            case Blocked _ -> buildBlocked = true;
                            case Data _ -> throw new IllegalStateException("Build should not emit data");
                            case Finished() -> throw new IllegalStateException("Unexpected finish");
                        }
                    }
                });

                // Drive probe with memory validation active
                Iterator<Page> probePages = createPages(types, nullsProvider, probePageCount, PROBE_ROWS_PER_PAGE, true);
                GpuLookupJoin.Factory probeFactory = new GpuLookupJoin.Factory(manager, keyChannels, probeOutputChannels, joinType, buildOutputTypes, filteredJoin);
                try (var probeSource = new SettableGpuOperation();
                        GpuOperation probe = probeFactory.create(context, probeSource)) {
                    boolean finished = false;
                    while (!finished) {
                        switch (probe.execute()) {
                            case Yielded() -> {
                                if (!probeSource.hasPending()) {
                                    memoryValidation.withoutValidation(() -> {
                                        if (probePages.hasNext()) {
                                            GpuPage next = getOnlyElement(copyToDevice(List.of(probePages.next()), types));
                                            probeSource.setPending(context.taskMemoryContext().allocate("probe", next.retainedMemory()), next);
                                        }
                                        else {
                                            probeSource.noMoreInput();
                                        }
                                    });
                                }
                            }
                            case Data(AllocatedMemory memory, GpuPage page) -> {
                                try (var closer = UncheckedCloser.create()) {
                                    closer.register(memory);
                                    closer.register(page);
                                }
                            }
                            case Blocked _ -> throw new IllegalStateException("Unexpected blocked on probe side");
                            case Finished() -> finished = true;
                        }
                    }
                }
                probeFactory.noMoreOperators();

                // Drive build to Finished after all probes done
                memoryValidation.withoutValidation(() -> {
                    assertThat(build.execute()).isInstanceOf(Finished.class);
                });
            }
        }
    }
}
