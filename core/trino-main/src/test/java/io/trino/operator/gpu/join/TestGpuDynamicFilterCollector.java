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
import ai.rapids.cudf.Table;
import io.airlift.units.DataSize;
import io.trino.operator.JoinDomainBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.gpu.Column.Blocks;
import io.trino.spi.gpu.GpuTypeConversion;
import io.trino.spi.gpu.GpuTypeConversion.GpuTypeMapping;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.TypeUtils;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.planner.DynamicFilterDomain;
import io.trino.sql.planner.DynamicFilterSourceConsumer;
import io.trino.sql.planner.DynamicFilterTupleDomain;
import io.trino.sql.planner.plan.DynamicFilterId;
import jakarta.annotation.Nullable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.trino.operator.gpu.GpuTestUtils.TESTED_GPU_TYPES;
import static io.trino.operator.gpu.GpuTestUtils.createBlock;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.sql.gen.TestColumnarFilters.NullsProvider.NO_NULLS;
import static org.assertj.core.api.Assertions.assertThat;

final class TestGpuDynamicFilterCollector
{
    private static final DynamicFilterId FILTER_ID = new DynamicFilterId("test");
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    private static final List<Type> ALL_TESTED_TYPES;
    private static final List<Type> ORDERABLE_TESTED_TYPES;

    static {
        ALL_TESTED_TYPES = TESTED_GPU_TYPES.stream()
                .filter(type -> GpuTypeConversion.toGpuMapping(type).map(mapping ->
                        mapping.fromScalar().isPresent()).orElse(false))
                .collect(toImmutableList());
        ORDERABLE_TESTED_TYPES = ALL_TESTED_TYPES.stream()
                .filter(Type::isOrderable)
                .filter(type -> type != REAL && type != DOUBLE && type != NUMBER)
                // CPU JoinDomainBuilder only collects min/max range as part of bloom filter path, which requires long-backed types
                .filter(type -> type.getJavaType() == long.class)
                .collect(toImmutableList());
    }

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testDiscreteValuesMatchCpu(NullsProvider nullsProvider)
    {
        for (Type type : ALL_TESTED_TYPES) {
            Block block = createBlock(type, 100, nullsProvider);

            DynamicFilterDomain cpuDomain = collectCpuDomain(type, block, 100, 0);
            DynamicFilterDomain gpuDomain = collectGpuDomain(type, block, 100);

            assertThat(gpuDomain)
                    .as("discrete domain for %s", type)
                    .isEqualTo(cpuDomain);
        }
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    void testRangeMatchesCpu(NullsProvider nullsProvider)
    {
        for (Type type : ORDERABLE_TESTED_TYPES) {
            Block block = createBlock(type, 100, nullsProvider);

            DynamicFilterDomain cpuDomain = collectCpuDomain(type, block, 5, 1000);
            DynamicFilterDomain gpuDomain = collectGpuDomain(type, block, 5);

            // Compare via toDomain() because the CPU wraps the range in a bloom filter while the GPU produces a plain range
            assertThat(gpuDomain.toDomain())
                    .as("range domain for %s", type)
                    .isEqualTo(cpuDomain.toDomain());
        }
    }

    @Test
    void testEmptyBuild()
    {
        DynamicFilterTupleDomain<DynamicFilterId> gpuResult = collectGpuTupleDomain(BIGINT, null, 100);

        assertThat(gpuResult.isNone())
                .as("empty build should produce none")
                .isTrue();

        DynamicFilterDomain cpuDomain = collectCpuDomain(BIGINT, null, 100, 0);
        assertThat(cpuDomain.isNone()).isTrue();
    }

    @Test
    void testDiscreteFloatWithNaN()
    {
        assertThat(TESTED_GPU_TYPES.stream().filter(TypeUtils::typeHasNaN))
                .containsExactlyInAnyOrder(REAL, DOUBLE);

        BlockBuilder realBuilder = REAL.createBlockBuilder(null, 3);
        REAL.writeFloat(realBuilder, 1.0f);
        REAL.writeFloat(realBuilder, Float.NaN);
        REAL.writeFloat(realBuilder, 2.0f);
        Block realBlock = realBuilder.build();

        DynamicFilterDomain cpuDomain = collectCpuDomain(REAL, realBlock, 100, 0);
        DynamicFilterDomain gpuDomain = collectGpuDomain(REAL, realBlock, 100);
        assertThat(gpuDomain)
                .as("REAL discrete with NaN")
                .isEqualTo(cpuDomain);

        BlockBuilder doubleBuilder = DOUBLE.createBlockBuilder(null, 3);
        DOUBLE.writeDouble(doubleBuilder, 1.0);
        DOUBLE.writeDouble(doubleBuilder, Double.NaN);
        DOUBLE.writeDouble(doubleBuilder, 2.0);
        Block doubleBlock = doubleBuilder.build();

        cpuDomain = collectCpuDomain(DOUBLE, doubleBlock, 100, 0);
        gpuDomain = collectGpuDomain(DOUBLE, doubleBlock, 100);
        assertThat(gpuDomain)
                .as("DOUBLE discrete with NaN")
                .isEqualTo(cpuDomain);
    }

    @Test
    void testHighCardinalityFloatProducesAll()
    {
        for (Type type : List.of(REAL, DOUBLE)) {
            Block block = createBlock(type, 100, NO_NULLS);

            DynamicFilterDomain cpuDomain = collectCpuDomain(type, block, 5, 0);
            assertThat(cpuDomain.isAll())
                    .as("CPU high cardinality %s", type)
                    .isTrue();

            DynamicFilterTupleDomain<DynamicFilterId> gpuResult = collectGpuTupleDomain(type, block, 5);
            assertThat(gpuResult.isAll())
                    .as("GPU high cardinality %s", type)
                    .isTrue();
        }
    }

    @Test
    void testVarbinaryProducesAll()
    {
        Block block = createBlock(VARBINARY, 5, NO_NULLS);

        DynamicFilterTupleDomain<DynamicFilterId> gpuResult = collectGpuTupleDomain(VARBINARY, block, 100);

        assertThat(gpuResult.isAll()).isTrue();
    }

    private static DynamicFilterDomain collectCpuDomain(Type type, @Nullable Block block, int maxDistinctValues, int bloomFilterMaxDistinctValues)
    {
        JoinDomainBuilder builder = new JoinDomainBuilder(
                type,
                maxDistinctValues,
                bloomFilterMaxDistinctValues,
                DataSize.of(10, KILOBYTE),
                () -> {},
                TYPE_OPERATORS);
        if (block != null) {
            builder.add(block);
        }
        return builder.build();
    }

    private static DynamicFilterDomain collectGpuDomain(Type type, Block block, int maxDistinctValues)
    {
        DynamicFilterTupleDomain<DynamicFilterId> tupleDomain = collectGpuTupleDomain(type, block, maxDistinctValues);
        return extractDomain(tupleDomain, type);
    }

    private static DynamicFilterTupleDomain<DynamicFilterId> collectGpuTupleDomain(Type type, @Nullable Block block, int maxDistinctValues)
    {
        GpuTypeMapping mapping = GpuTypeConversion.toGpuMapping(type).orElseThrow();

        GpuDynamicFilterCollector.Channel channel = new GpuDynamicFilterCollector.Channel(
                FILTER_ID, 0, type, mapping.fromScalar());

        AtomicReference<DynamicFilterTupleDomain<DynamicFilterId>> result = new AtomicReference<>();
        DynamicFilterSourceConsumer consumer = captureConsumer(result);

        GpuDynamicFilterCollector collector = new GpuDynamicFilterCollector(
                consumer, List.of(channel), maxDistinctValues);

        if (block == null) {
            collector.collectEmpty();
        }
        else {
            try (Blocks blocks = new Blocks(List.of(block));
                    ColumnVector deviceColumn = mapping.toColumn().copyToDevice(blocks);
                    Table table = new Table(deviceColumn)) {
                collector.collect(table);
            }
        }

        assertThat(result.get()).isNotNull();
        return result.get();
    }

    private static DynamicFilterDomain extractDomain(DynamicFilterTupleDomain<DynamicFilterId> tupleDomain, Type type)
    {
        if (tupleDomain.isNone()) {
            return DynamicFilterDomain.none(type);
        }
        Map<DynamicFilterId, DynamicFilterDomain> domains = tupleDomain.getDomains().orElseThrow();
        assertThat(domains).containsKey(FILTER_ID);
        return domains.get(FILTER_ID);
    }

    private static DynamicFilterSourceConsumer captureConsumer(AtomicReference<DynamicFilterTupleDomain<DynamicFilterId>> result)
    {
        return new DynamicFilterSourceConsumer()
        {
            @Override
            public void addPartition(DynamicFilterTupleDomain<DynamicFilterId> tupleDomain)
            {
                result.set(tupleDomain);
            }

            @Override
            public void setPartitionCount(int partitionCount) {}

            @Override
            public boolean isDomainCollectionComplete()
            {
                return false;
            }
        };
    }
}
