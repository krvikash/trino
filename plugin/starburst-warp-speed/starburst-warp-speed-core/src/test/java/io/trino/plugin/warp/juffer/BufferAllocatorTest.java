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
package io.trino.plugin.warp.juffer;

import io.trino.plugin.warp.WarmColumnDataTestUtil;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.config.SharedConfig;
import io.trino.plugin.warp.di.WarpInitializedServiceRegistry;
import io.trino.plugin.warp.dispatcher.WarmupElementWriteMetadata;
import io.trino.plugin.warp.dispatcher.model.RecordData;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.engine.StubsStorageEngineConstants;
import io.trino.plugin.warp.storage.memory.WorkerMemoryManager;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.plugin.warp.WarmColumnDataTestUtil.generateRecordData;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class BufferAllocatorTest
{
    public static final int MAX_COLS = 10;
    private static final int CHUNK_SIZE_SHIFT = 16;
    private static final int REC_MAX_SIZE = 1 << 19;
    private static final int PAGE_SIZE = 8192;
    private BufferAllocator bufferAllocator;
    private NativeConfig nativeConfig;
    private StorageEngineConstants storageEngineConstants;

    @BeforeEach
    public void before()
    {
        nativeConfig = mock(NativeConfig.class);
        when(nativeConfig.getTaskMaxWorkerThreads()).thenReturn(4);

        storageEngineConstants = spy(new StubsStorageEngineConstants());
        initStorageEngineConstants(storageEngineConstants);

        StorageEngine storageEngine = mock(StorageEngine.class);

        CatalogName catalogName = new CatalogName("f");
        bufferAllocator = new BufferAllocator(
                storageEngine,
                storageEngineConstants,
                nativeConfig,
                new WorkerMemoryManager(catalogName, new ShapingLoggerFactory(new CatalogName("c"), new SharedConfig())),
                mock(MetricsManager.class),
                new WarpInitializedServiceRegistry(),
                catalogName);
    }

    @AfterEach
    public void after()
    {
        bufferAllocator.clear();
    }

    private void initStorageEngineConstants(StorageEngineConstants storageEngineConstants)
    {
        doReturn(253).when(storageEngineConstants).getVarlenExtLimit();
        doReturn(7 * 8192).when(storageEngineConstants).getVarcharMaxLen();
        doReturn(CHUNK_SIZE_SHIFT).when(storageEngineConstants).getChunkSizeShift();
        doReturn(PAGE_SIZE).when(storageEngineConstants).getPageSize();
        doReturn(REC_MAX_SIZE / 4).when(storageEngineConstants).getRecordBufferMaxSize();
        doReturn(8).when(storageEngineConstants).getFixedLengthStringLimit();
        doReturn(MAX_COLS).when(storageEngineConstants).getMaxMatchColumns();
    }

    @Test
    public void testCalculateGreaterThen8VarcharAndSmallerThenMaxExtLimit()
    {
        RecordData recordData = generateRecordData("col", VarcharType.createVarcharType(253));
        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmUpElementWithDictionary(recordData, WarmUpType.WARM_UP_TYPE_DATA);
        WarmUpElementAllocationParams warmUpElementAllocationParams = bufferAllocator.calculateAllocationParams(warmupElementWriteMetadata, null);
        assertThat(warmUpElementAllocationParams.extRecBuffSize()).isEqualTo(0);
    }

    @Test
    public void testPredicateBufferAllocation()
    {
        StorageEngine storageEngine = mock(StorageEngine.class);
        CatalogName catalogName = new CatalogName("f");
        BufferAllocator bufferAllocator = new BufferAllocator(
                storageEngine,
                storageEngineConstants,
                nativeConfig,
                new WorkerMemoryManager(catalogName, new ShapingLoggerFactory(new CatalogName("c"), new SharedConfig())),
                mock(MetricsManager.class),
                new WarpInitializedServiceRegistry(),
                catalogName);
        // pre-alloc
        bufferAllocator.init();
        PredicateBufferInfo predicateBufferInfo = requireNonNull(bufferAllocator.allocPredicateBuffer(10));
        PredicateCacheData bufferHandleSmall = new PredicateCacheData(predicateBufferInfo, Optional.empty());
        assertThat(bufferHandleSmall.getPredicateBufferInfo().buff().byteSize()).isEqualTo(100);
        assertThat(bufferHandleSmall.getPredicateBufferInfo().predicateBufferPoolType()).isEqualTo(PredicateBufferPoolType.TINY);
        PredicateBufferInfo predicateBufferInfo2 = requireNonNull(bufferAllocator.allocPredicateBuffer(70));
        PredicateCacheData bufferHandleSmall2 = new PredicateCacheData(predicateBufferInfo2, Optional.empty());
        assertThat(bufferHandleSmall2.getPredicateBufferInfo().buff().byteSize()).isEqualTo(100);
        assertThat(bufferHandleSmall2.getPredicateBufferInfo().predicateBufferPoolType()).isEqualTo(PredicateBufferPoolType.TINY);

        // small/large
        PredicateCacheData bufferHandleMedium1 = createPredicateCacheData(bufferAllocator, 3000);
        assertThat(bufferHandleMedium1.getPredicateBufferInfo().predicateBufferPoolType()).isEqualTo(PredicateBufferPoolType.MEDIUM);
        assertThat(bufferHandleMedium1.getPredicateBufferInfo().buff().byteSize()).isEqualTo(100000);
        PredicateCacheData bufferHandleMedium2 = createPredicateCacheData(bufferAllocator, 40000);
        assertThat(bufferHandleMedium2.getPredicateBufferInfo().predicateBufferPoolType()).isEqualTo(PredicateBufferPoolType.MEDIUM);
        PredicateCacheData bufferHandleMedium3 = createPredicateCacheData(bufferAllocator, 100000);
        assertThat(bufferHandleMedium3.getPredicateBufferInfo().predicateBufferPoolType()).isEqualTo(PredicateBufferPoolType.MEDIUM);
        PredicateCacheData bufferHandleLarge = createPredicateCacheData(bufferAllocator, 999999);
        assertThat(bufferHandleLarge.getPredicateBufferInfo().predicateBufferPoolType()).isEqualTo(PredicateBufferPoolType.LARGE);
        bufferAllocator.freePredicateBuffer(bufferHandleMedium3);
        bufferAllocator.freePredicateBuffer(bufferHandleLarge);
        bufferAllocator.freePredicateBuffer(bufferHandleMedium1);
        // left one small buffer allocated

        // allocate one more free and free all
        PredicateCacheData bufferHandleMedium4 = createPredicateCacheData(bufferAllocator, 50000);
        assertThat(bufferHandleMedium4.getPredicateBufferInfo().predicateBufferPoolType()).isEqualTo(PredicateBufferPoolType.MEDIUM);
        bufferAllocator.freePredicateBuffer(bufferHandleMedium2);
        bufferAllocator.freePredicateBuffer(bufferHandleMedium4);

        // allocate large again an free it
        PredicateCacheData bufferHandleLarge2 = createPredicateCacheData(bufferAllocator, 900000);
        assertThat(bufferHandleLarge2.getPredicateBufferInfo().predicateBufferPoolType()).isEqualTo(PredicateBufferPoolType.LARGE);
        bufferAllocator.freePredicateBuffer(bufferHandleLarge2);

        bufferAllocator.clear();
    }

    public PredicateCacheData createPredicateCacheData(BufferAllocator bufferAllocator, int size)
    {
        PredicateBufferInfo predicateBufferInfo = requireNonNull(bufferAllocator.allocPredicateBuffer(size));
        return new PredicateCacheData(predicateBufferInfo, Optional.empty());
    }
}
