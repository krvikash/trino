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
package io.trino.plugin.warp.storage.write;

import io.trino.plugin.warp.TestingTxService;
import io.trino.plugin.warp.WarmColumnDataTestUtil;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.config.SharedConfig;
import io.trino.plugin.warp.dispatcher.WarmupElementWriteMetadata;
import io.trino.plugin.warp.dispatcher.warmup.transform.BlockTransformerFactory;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.metrics.PrintMetricsTimerTask;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.engine.StubsStorageEngine;
import io.trino.plugin.warp.storage.engine.StubsStorageEngineConstants;
import io.trino.plugin.warp.storage.juffers.BaseJuffer;
import io.trino.plugin.warp.storage.juffers.WriteJuffersWarmUpElement;
import io.trino.plugin.warp.storage.lucene.LuceneIndexer;
import io.trino.plugin.warp.storage.memory.WorkerMemoryManager;
import io.trino.plugin.warp.storage.write.appenders.BlockAppenderFactory;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlockBuilder;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.VarcharType;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.stream.IntStream;

import static io.trino.plugin.warp.dispatcher.WarmupTestDataUtil.mockBufferAllocator;
import static io.trino.plugin.warp.dispatcher.warmup.warmers.StorageWarmerService.INVALID_FILE_COOKIE_FD;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class StorageWriterServiceTest
{
    private static final int SHOULD_BE_NULL = -1;

    private final String rowGroupFilePath = "/tmp/testStorageWriterService/schema/table/column/offset/length/";

    private StorageWriterService storageWriterService;
    private BufferAllocator bufferAllocator;

    public static Page buildLongPage(long... values)
    {
        LongArrayBlockBuilder block = new LongArrayBlockBuilder(null, values.length);
        IntStream.range(0, values.length).forEach(i -> block.writeLong(values[i]));
        return new Page(block.build());
    }

    public static Page buildVarcharPage(String... values)
    {
        VarcharType unboundedVarcharType = createUnboundedVarcharType();
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, values.length, values.length);
        for (String value : values) {
            unboundedVarcharType.writeString(blockBuilder, value);
        }
        Block block = blockBuilder.build();

        return new Page(block);
    }

    @BeforeEach
    public void before()
    {
        initiate(new StubsStorageEngine(), new StubsStorageEngineConstants());
    }

    @AfterEach
    public void after()
            throws IOException
    {
        bufferAllocator.clear();
        FileUtils.deleteDirectory(new File("/tmp/test/StorageWriterService"));
    }

    private void initiate(StorageEngine storageEngineToSpy, StubsStorageEngineConstants storageEngineConstants)
    {
        StorageEngine storageEngine = spy(storageEngineToSpy);
        MetricsManager metricsManager = TestingTxService.createMetricsManager();

        NativeConfig nativeConfig = new NativeConfig();
        nativeConfig.setTaskMaxWorkerThreads(4);
        nativeConfig.setLimitNumIosInParallel(100);
        nativeConfig.setMaxIOMetadataSize(8);

        this.bufferAllocator = mockBufferAllocator(storageEngine, storageEngineConstants, nativeConfig, metricsManager);
        BlockTransformerFactory blockTransformerFactory = new BlockTransformerFactory();
        BlockAppenderFactory blockAppenderFactory = new BlockAppenderFactory(storageEngineConstants, bufferAllocator, blockTransformerFactory);

        CatalogName catalogName = new CatalogName("f");
        WarmupElementStatsService warmupElementStatsService = new WarmupElementStatsService(new ShapingLoggerFactory(catalogName, new SharedConfig()));
        WorkerMemoryManager workerMemoryManager = new WorkerMemoryManager(catalogName, new ShapingLoggerFactory(catalogName, new SharedConfig()));
        storageWriterService = new StorageWriterService(
                storageEngine,
                storageEngineConstants,
                bufferAllocator,
                metricsManager,
                mock(PrintMetricsTimerTask.class),
                blockAppenderFactory,
                warmupElementStatsService,
                workerMemoryManager,
                nativeConfig,
                new ShapingLoggerFactory(catalogName, new SharedConfig()));
    }

    @Test
    public void writeVarcharIndex()
    {
        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmupElementWriteMetadata(WarmColumnDataTestUtil.generateRecordData(
                        "col1",
                        VarcharType.createVarcharType(9)),
                WarmUpType.WARM_UP_TYPE_BASIC);
        String[] values = {"a", "A"};
        Page page = buildVarcharPage(values);
        WriteOpenResult writeOpenResult = runTest(page, warmupElementWriteMetadata);
        WriteJuffersWarmUpElement dataRecordJuffer = writeOpenResult.storageWriterContext().getWriteJuffersWarmUpElement();

        ByteBuffer actualRecBuffer = dataRecordJuffer.getCrcBuffer();
        int expectedPosition = values.length * (8 + 2);

        assertBuffer(dataRecordJuffer.getCrcJuffer(), expectedPosition);

        assertPositionResults(dataRecordJuffer, 2, SHOULD_BE_NULL);
        assertThat(actualRecBuffer.position()).isEqualTo(expectedPosition);
    }

    @Test
    public void writeVarcharWithLucene()
    {
        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmupElementWriteMetadata(WarmColumnDataTestUtil.generateRecordData(
                        "col1",
                        VarcharType.createVarcharType(9)),
                WarmUpType.WARM_UP_TYPE_LUCENE);

        String[] values = {"a", "b"};
        Page page = buildVarcharPage(values);
        StorageWriterSplitConfig storageWriterSplitConfig = startWarming("writeVarcharWithLucene");
        WriteOpenResult writeOpenResult = txCreate(storageWriterSplitConfig, warmupElementWriteMetadata);
        StorageWriterContext storageWriterContext = writeOpenResult.storageWriterContext();
        storageWriterService.appendPage(SourcePage.create(page), storageWriterContext);

        LuceneIndexer luceneIndexer = storageWriterContext.getLuceneIndexer().orElseThrow();
        Directory directory = luceneIndexer.getLuceneDirectory();
        assertThat(directory).isNotNull();

        storageWriterService.close(page.getPositionCount(), storageWriterSplitConfig, storageWriterContext);
        assertThatThrownBy(directory::listAll).isInstanceOf(AlreadyClosedException.class); // since it finish will throw AlreadyClosedException.class
    }

    @Test
    public void abortVarcharWithLucene()
    {
        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmupElementWriteMetadata(WarmColumnDataTestUtil.generateRecordData(
                        "col1",
                        VarcharType.createVarcharType(9)),
                WarmUpType.WARM_UP_TYPE_LUCENE);

        String[] values = {"a"};
        Page page = buildVarcharPage(values);
        StorageWriterSplitConfig storageWriterSplitConfig = startWarming("abortVarcharWithLucene");
        WriteOpenResult writeOpenResult = txCreate(storageWriterSplitConfig, warmupElementWriteMetadata);
        StorageWriterContext storageWriterContext = writeOpenResult.storageWriterContext();
        storageWriterService.appendPage(SourcePage.create(page), storageWriterContext);

        LuceneIndexer luceneIndexer = storageWriterContext.getLuceneIndexer().orElseThrow();
        Directory directory = luceneIndexer.getLuceneDirectory();
        assertThat(directory).isNotNull();

        storageWriterService.abort(false, storageWriterContext, storageWriterSplitConfig);
        assertThatThrownBy(directory::listAll).isInstanceOf(AlreadyClosedException.class); // since it finish will throw AlreadyClosedException.class
    }

    private WriteOpenResult txCreate(StorageWriterSplitConfig storageWriterSplitConfig, WarmupElementWriteMetadata warmupElementWriteMetadata)
    {
        return storageWriterService.open(new long[] {INVALID_FILE_COOKIE_FD, 0, 0}, 0, storageWriterSplitConfig, warmupElementWriteMetadata);
    }

    private WriteOpenResult runTest(Page page, WarmupElementWriteMetadata warmupElementWriteMetadata)
    {
        StorageWriterSplitConfig storageWriterSplitConfig = startWarming("runTest");
        WriteOpenResult writeOpenResult = txCreate(storageWriterSplitConfig, warmupElementWriteMetadata);
        storageWriterService.appendPage(SourcePage.create(page), writeOpenResult.storageWriterContext());
        return writeOpenResult;
    }

    private StorageWriterSplitConfig startWarming(String suffix)
    {
        return storageWriterService.startWarming(
                "nodeIdentifier",
                rowGroupFilePath + suffix,
                true);
    }

    public void assertPositionResults(WriteJuffersWarmUpElement dataRecordJuffer, int expectedNullPosition, int expectedMdPosition)
    {
        assertBuffer(dataRecordJuffer.getNullJuffer(), expectedNullPosition);
        assertBuffer(dataRecordJuffer.getVarlenMdJuffer(), expectedMdPosition);
    }

    private void assertBuffer(BaseJuffer juffer, int expectedPosition)
    {
        if (expectedPosition == SHOULD_BE_NULL) {
            assertThat(juffer).isNull();
        }
        else {
            Buffer buffer = juffer.getWrappedBuffer();
            assertThat(buffer.position()).isEqualTo(expectedPosition);
        }
    }
}
