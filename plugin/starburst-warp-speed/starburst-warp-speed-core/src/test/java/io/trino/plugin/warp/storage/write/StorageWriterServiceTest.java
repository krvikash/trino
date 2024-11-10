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
import io.trino.plugin.warp.config.DictionaryConfig;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.config.SharedConfig;
import io.trino.plugin.warp.dictionary.DictionaryCacheService;
import io.trino.plugin.warp.dispatcher.WarmupElementWriteMetadata;
import io.trino.plugin.warp.dispatcher.cache.WarmupElementBlocks;
import io.trino.plugin.warp.dispatcher.model.DictionaryState;
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
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.IntArrayBlockBuilder;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.block.LongArrayBlockBuilder;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
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
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static io.trino.plugin.warp.dispatcher.WarmupTestDataUtil.mockBufferAllocator;
import static io.trino.plugin.warp.dispatcher.warmup.warmers.StorageWarmerService.INVALID_FILE_COOKIE_FD;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class StorageWriterServiceTest
{
    private static final int SHOULD_BE_NULL = -1;

    private final String rowGroupFilePath = "/tmp/testStorageWriterService/schema/table/column/offset/length/";

    private DictionaryCacheService dictionaryCacheService;
    private StorageWriterService storageWriterService;
    private BufferAllocator bufferAllocator;

    public static Page buildIntPage(int... values)
    {
        IntArrayBlockBuilder block = new IntArrayBlockBuilder(null, values.length);
        IntStream.range(0, values.length).forEach((i) -> block.writeInt(values[i]));
        return new Page(block.build());
    }

    public static Page buildLongPage(long... values)
    {
        LongArrayBlockBuilder block = new LongArrayBlockBuilder(null, values.length);
        IntStream.range(0, values.length).forEach((i) -> block.writeLong(values[i]));
        LazyBlock lazyBlock = new LazyBlock(values.length, block::build);
        return new Page(lazyBlock);
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
        GlobalConfig globalConfig = new GlobalConfig();
        StorageEngine storageEngine = spy(storageEngineToSpy);
        MetricsManager metricsManager = TestingTxService.createMetricsManager();

        DictionaryConfig dictionaryConfig = new DictionaryConfig();
        dictionaryConfig.setDictionaryCacheConcurrencyLevel(1);

        NativeConfig nativeConfig = new NativeConfig();
        nativeConfig.setTaskMaxWorkerThreads(4);
        nativeConfig.setLimitNumIosInParallel(100);
        nativeConfig.setMaxIOMetadataSize(8);

        this.bufferAllocator = mockBufferAllocator(storageEngine, storageEngineConstants, nativeConfig, metricsManager);
        dictionaryCacheService = mock(DictionaryCacheService.class);
        BlockTransformerFactory blockTransformerFactory = new BlockTransformerFactory();
        BlockAppenderFactory blockAppenderFactory = new BlockAppenderFactory(storageEngineConstants, bufferAllocator, globalConfig, blockTransformerFactory);

        CatalogName catalogName = new CatalogName("f");
        WarmupElementStatsService warmupElementStatsService = new WarmupElementStatsService(new ShapingLoggerFactory(catalogName, new SharedConfig()));
        WorkerMemoryManager workerMemoryManager = new WorkerMemoryManager(catalogName, new ShapingLoggerFactory(catalogName, new SharedConfig()));
        storageWriterService = new StorageWriterService(storageEngine,
                storageEngineConstants,
                bufferAllocator,
                dictionaryCacheService,
                metricsManager,
                mock(PrintMetricsTimerTask.class),
                blockAppenderFactory,
                warmupElementStatsService,
                workerMemoryManager,
                nativeConfig,
                new ShapingLoggerFactory(catalogName, new SharedConfig()));
    }

    @Test
    public void writeInt()
    {
        when(dictionaryCacheService.calculateDictionaryStateForWrite(any(), any(), any())).thenReturn(DictionaryState.DICTIONARY_NOT_EXIST);
        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmUpElementWithDictionary(WarmColumnDataTestUtil.generateRecordData("col1", IntegerType.INTEGER), WarmUpType.WARM_UP_TYPE_DATA);
        int[] values = new int[] {1, 2, 3};
        Page page = buildIntPage(values);
        WriteOpenResult writeOpenResult = runTest(page, warmupElementWriteMetadata);
        WriteJuffersWarmUpElement dataRecordJuffer = writeOpenResult.storageWriterContext().getWriteJuffersWarmUpElement();
        IntBuffer actualRecBuffer = (IntBuffer) dataRecordJuffer.getRecordBuffer();

        assertPositionResults(dataRecordJuffer, values.length, SHOULD_BE_NULL);
        assertThat(actualRecBuffer.position()).isEqualTo(values.length);
        actualRecBuffer.position(0);
        int[] writtenValues = new int[values.length];
        actualRecBuffer.get(writtenValues, 0, values.length);
        assertThat(values).isEqualTo(writtenValues);
        assertThat(writeOpenResult.dictionaryInfo().dictionaryState()).isEqualTo(DictionaryState.DICTIONARY_NOT_EXIST);
    }

    @Test
    public void writeReal()
    {
        when(dictionaryCacheService.calculateDictionaryStateForWrite(any(), any(), any())).thenReturn(DictionaryState.DICTIONARY_NOT_EXIST);
        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmUpElementWithDictionary(WarmColumnDataTestUtil.generateRecordData("col1", RealType.REAL), WarmUpType.WARM_UP_TYPE_DATA);
        int[] values = new int[] {1, 2, 3};
        Page page = buildIntPage(values);
        WriteOpenResult writeOpenResult = runTest(page, warmupElementWriteMetadata);
        WriteJuffersWarmUpElement dataRecordJuffer = writeOpenResult.storageWriterContext().getWriteJuffersWarmUpElement();

        assertPositionResults(dataRecordJuffer, values.length, SHOULD_BE_NULL);

        IntBuffer actualRecBuffer = (IntBuffer) dataRecordJuffer.getRecordBuffer();
        assertThat(actualRecBuffer.position()).isEqualTo(values.length);
        actualRecBuffer.position(0);
        int[] writtenValues = new int[values.length];
        actualRecBuffer.get(writtenValues, 0, values.length);
        assertThat(values).isEqualTo(writtenValues);
    }

    @Test
    public void writeLong()
    {
        when(dictionaryCacheService.calculateDictionaryStateForWrite(any(), any(), any())).thenReturn(DictionaryState.DICTIONARY_NOT_EXIST);
        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmUpElementWithDictionary(WarmColumnDataTestUtil.generateRecordData("col1", BIGINT), WarmUpType.WARM_UP_TYPE_DATA);

        long[] values = new long[] {1, 2, 3};
        Page page = buildLongPage(values);
        WriteOpenResult writeOpenResult = runTest(page, warmupElementWriteMetadata);
        WriteJuffersWarmUpElement dataRecordJuffer = writeOpenResult.storageWriterContext().getWriteJuffersWarmUpElement();

        assertPositionResults(dataRecordJuffer, values.length, SHOULD_BE_NULL);

        LongBuffer actualRecBuffer = (LongBuffer) dataRecordJuffer.getRecordBuffer();
        assertThat(actualRecBuffer.position()).isEqualTo(values.length);
        actualRecBuffer.position(0);
        long[] writtenValues = new long[values.length];
        actualRecBuffer.get(writtenValues, 0, values.length);
        assertThat(values).isEqualTo(writtenValues);
    }

    @Test
    public void writeArrayTypeArrayOfInteger()
    {
        when(dictionaryCacheService.calculateDictionaryStateForWrite(any(), any(), any())).thenReturn(DictionaryState.DICTIONARY_NOT_EXIST);
        ArrayType arrayIntType = new ArrayType(IntegerType.INTEGER);
        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmUpElementWithDictionary(WarmColumnDataTestUtil.generateRecordData("col1", arrayIntType), WarmUpType.WARM_UP_TYPE_DATA);
        int[] values = new int[] {1, 2, 3};
        Page page = buildArrayType_IntPage(values);
        WriteOpenResult writeOpenResult = runTest(page, warmupElementWriteMetadata);
        WriteJuffersWarmUpElement dataRecordJuffer = writeOpenResult.storageWriterContext().getWriteJuffersWarmUpElement();

        assertPositionResults(dataRecordJuffer, 1, 1);
    }

    @Test
    public void writeArrayTypeArrayOfBigInt()
    {
        when(dictionaryCacheService.calculateDictionaryStateForWrite(any(), any(), any())).thenReturn(DictionaryState.DICTIONARY_NOT_EXIST);
        ArrayType arrayBigIntType = new ArrayType(BIGINT);
        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmUpElementWithDictionary(WarmColumnDataTestUtil.generateRecordData("col1", arrayBigIntType), WarmUpType.WARM_UP_TYPE_DATA);

        long[][] values = new long[][] {
                new long[] {Long.MAX_VALUE, 7, Long.MIN_VALUE},
                new long[] {1, 2, 3}
        };

        Page page = buildArrayType_BigIntPage(values);
        WriteOpenResult writeOpenResult = runTest(page, warmupElementWriteMetadata);
        WriteJuffersWarmUpElement dataRecordJuffer = writeOpenResult.storageWriterContext().getWriteJuffersWarmUpElement();
        assertPositionResults(dataRecordJuffer, 2, 1);
    }

    @Test
    public void writeVarcharArray_EmptyArray()
    {
        when(dictionaryCacheService.calculateDictionaryStateForWrite(any(), any(), any())).thenReturn(DictionaryState.DICTIONARY_NOT_EXIST);
        ArrayType varcharArrayType = new ArrayType(VARCHAR);
        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmUpElementWithDictionary(WarmColumnDataTestUtil.generateRecordData("col1",
                        varcharArrayType),
                WarmUpType.WARM_UP_TYPE_DATA);
        String[][] values = new String[][] {
        };

        Page page = buildArrayType_VarcharPage(values);
        WriteOpenResult writeOpenResult = runTest(page, warmupElementWriteMetadata);
        WriteJuffersWarmUpElement dataRecordJuffer = writeOpenResult.storageWriterContext().getWriteJuffersWarmUpElement();
        assertPositionResults(dataRecordJuffer, values.length, 0);
    }

    @Test
    public void writeVarcharArray()
    {
        when(dictionaryCacheService.calculateDictionaryStateForWrite(any(), any(), any())).thenReturn(DictionaryState.DICTIONARY_NOT_EXIST);
        ArrayType varcharArrayType = new ArrayType(VARCHAR);
        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmUpElementWithDictionary(WarmColumnDataTestUtil.generateRecordData("col1", varcharArrayType), WarmUpType.WARM_UP_TYPE_DATA);

        String[][] values = new String[][] {
                new String[] {"1", "22", "33"},
                new String[] {"333", "4444"},
                new String[] {"5"},
        };

        Page page = buildArrayType_VarcharPage(values);
        WriteOpenResult writeOpenResult = runTest(page, warmupElementWriteMetadata);
        WriteJuffersWarmUpElement dataRecordJuffer = writeOpenResult.storageWriterContext().getWriteJuffersWarmUpElement();
        assertPositionResults(dataRecordJuffer, values.length, 1);

        ByteBuffer recordBuffer = (ByteBuffer) dataRecordJuffer.getRecordBuffer();
        assertThat(recordBuffer.getInt(1)).isEqualTo(values[0].length);
    }

    @Test
    public void writeVarcharArrayTest_TestNullBuffer()
    {
        when(dictionaryCacheService.calculateDictionaryStateForWrite(any(), any(), any())).thenReturn(DictionaryState.DICTIONARY_NOT_EXIST);
        ArrayType varcharArrayType = new ArrayType(VARCHAR);
        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmUpElementWithDictionary(WarmColumnDataTestUtil.generateRecordData("col1", varcharArrayType), WarmUpType.WARM_UP_TYPE_DATA);

        String[][] values = new String[][] {
                new String[] {null}
        };

        Page page = buildArrayType_VarcharPage(values);
        WriteOpenResult writeOpenResult = runTest(page, warmupElementWriteMetadata);
        WriteJuffersWarmUpElement dataRecordJuffer = writeOpenResult.storageWriterContext().getWriteJuffersWarmUpElement();
        assertPositionResults(dataRecordJuffer, 1, 1);
        ByteBuffer recordBuffer = (ByteBuffer) dataRecordJuffer.getRecordBuffer();
        assertThat(recordBuffer.getInt(1)).isEqualTo(values[0].length);
    }

    @Test
    public void writeVarcharArrayTest_TestNullAtEndOfRow()
    {
        when(dictionaryCacheService.calculateDictionaryStateForWrite(any(), any(), any())).thenReturn(DictionaryState.DICTIONARY_NOT_EXIST);
        ArrayType varcharArrayType = new ArrayType(VarcharType.VARCHAR);
        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmUpElementWithDictionary(WarmColumnDataTestUtil.generateRecordData("col1", varcharArrayType), WarmUpType.WARM_UP_TYPE_DATA);

        String[][] values = new String[][] {
                new String[] {"a", null}
        };

        Page page = buildArrayType_VarcharPage(values);
        WriteOpenResult writeOpenResult = runTest(page, warmupElementWriteMetadata);
        WriteJuffersWarmUpElement dataRecordJuffer = writeOpenResult.storageWriterContext().getWriteJuffersWarmUpElement();
        assertPositionResults(dataRecordJuffer, 1, 1);
        ByteBuffer recordBuffer = (ByteBuffer) dataRecordJuffer.getRecordBuffer();
        assertThat(recordBuffer.getInt(1)).isEqualTo(values[0].length);
    }

    @Test
    public void writeVarchar_varcharIsSmallerThanVarcharAsCharLimit()
    {
        when(dictionaryCacheService.calculateDictionaryStateForWrite(any(), any(), any())).thenReturn(DictionaryState.DICTIONARY_NOT_EXIST);
        final int typeLength = 5;
        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmUpElementWithDictionary(WarmColumnDataTestUtil.generateRecordData("col1",
                        VarcharType.createVarcharType(typeLength)),
                WarmUpType.WARM_UP_TYPE_DATA);

        String[] values = new String[] {"a", "A"};
        Page page = buildVarcharPage(values);
        WriteOpenResult writeOpenResult = runTest(page, warmupElementWriteMetadata);
        WriteJuffersWarmUpElement dataRecordJuffer = writeOpenResult.storageWriterContext().getWriteJuffersWarmUpElement();

        ByteBuffer actualRecBuffer = (ByteBuffer) dataRecordJuffer.getRecordBuffer();

        assertPositionResults(dataRecordJuffer, values.length, SHOULD_BE_NULL);
        assertThat(actualRecBuffer.position()).isEqualTo(typeLength * values.length);
    }

    @Test
    public void writeVarchar()
    {
        when(dictionaryCacheService.calculateDictionaryStateForWrite(any(), any(), any())).thenReturn(DictionaryState.DICTIONARY_NOT_EXIST);
        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmUpElementWithDictionary(WarmColumnDataTestUtil.generateRecordData("col1",
                        VarcharType.createVarcharType(9)),
                WarmUpType.WARM_UP_TYPE_DATA);
        String[] values = new String[] {"a", "A"};
        Page page = buildVarcharPage(values);
        WriteOpenResult writeOpenResult = runTest(page, warmupElementWriteMetadata);
        WriteJuffersWarmUpElement dataRecordJuffer = writeOpenResult.storageWriterContext().getWriteJuffersWarmUpElement();

        ByteBuffer actualRecBuffer = (ByteBuffer) dataRecordJuffer.getRecordBuffer();

        assertPositionResults(dataRecordJuffer, values.length, 1);
        assertThat(actualRecBuffer.position()).isEqualTo(2 * values.length);
    }

    @Test
    public void writeVarcharIndex()
    {
        when(dictionaryCacheService.calculateDictionaryStateForWrite(any(), any(), any())).thenReturn(DictionaryState.DICTIONARY_NOT_EXIST);
        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmUpElementWithDictionary(WarmColumnDataTestUtil.generateRecordData("col1",
                        VarcharType.createVarcharType(9)),
                WarmUpType.WARM_UP_TYPE_BASIC);
        String[] values = new String[] {"a", "A"};
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
        when(dictionaryCacheService.calculateDictionaryStateForWrite(any(), any(), any())).thenReturn(DictionaryState.DICTIONARY_NOT_EXIST);
        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmUpElementWithDictionary(WarmColumnDataTestUtil.generateRecordData("col1",
                        VarcharType.createVarcharType(9)),
                WarmUpType.WARM_UP_TYPE_LUCENE);

        String[] values = new String[] {"a", "b"};
        Page page = buildVarcharPage(values);
        StorageWriterSplitConfig storageWriterSplitConfig = startWarming("writeVarcharWithLucene");
        WriteOpenResult writeOpenResult = txCreate(storageWriterSplitConfig, warmupElementWriteMetadata);
        StorageWriterContext storageWriterContext = writeOpenResult.storageWriterContext();
        storageWriterService.appendPage(SourcePage.create(page), storageWriterContext);

        LuceneIndexer luceneIndexer = storageWriterContext.getLuceneIndexer().orElseThrow();
        Directory directory = luceneIndexer.getLuceneDirectory();
        assertThat(directory).isNotNull();

        storageWriterService.close(page.getPositionCount(), storageWriterSplitConfig, storageWriterContext);
        assertThatThrownBy(directory::listAll).isInstanceOf(AlreadyClosedException.class); //since it finish will throw AlreadyClosedException.class
    }

    @Test
    public void abortVarcharWithLucene()
    {
        when(dictionaryCacheService.calculateDictionaryStateForWrite(any(), any(), any())).thenReturn(DictionaryState.DICTIONARY_NOT_EXIST);
        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmUpElementWithDictionary(WarmColumnDataTestUtil.generateRecordData("col1",
                        VarcharType.createVarcharType(9)),
                WarmUpType.WARM_UP_TYPE_LUCENE);

        String[] values = new String[] {"a"};
        Page page = buildVarcharPage(values);
        StorageWriterSplitConfig storageWriterSplitConfig = startWarming("abortVarcharWithLucene");
        WriteOpenResult writeOpenResult = txCreate(storageWriterSplitConfig, warmupElementWriteMetadata);
        StorageWriterContext storageWriterContext = writeOpenResult.storageWriterContext();
        storageWriterService.appendPage(SourcePage.create(page), storageWriterContext);

        LuceneIndexer luceneIndexer = storageWriterContext.getLuceneIndexer().orElseThrow();
        Directory directory = luceneIndexer.getLuceneDirectory();
        assertThat(directory).isNotNull();

        storageWriterService.abort(false, storageWriterContext, storageWriterSplitConfig);
        assertThatThrownBy(directory::listAll).isInstanceOf(AlreadyClosedException.class); //since it finish will throw AlreadyClosedException.class
    }

    @Test
    public void testAppendWarmupElementBlocksNotReady()
    {
        int chunkSize = 10;
        int numberOfBlocks = 2;
        int recordsPerBlock = 3;

        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmUpElementWithDictionary(
                WarmColumnDataTestUtil.generateRecordData("col1", BIGINT),
                WarmUpType.WARM_UP_TYPE_DATA);

        StorageWriterSplitConfig storageWriterSplitConfig = startWarming("testAppendWarmupElementBlocksNotReady");
        WriteOpenResult writeOpenResult = txCreate(storageWriterSplitConfig, warmupElementWriteMetadata);
        StorageWriterContext storageWriterContext = writeOpenResult.storageWriterContext();
        storageWriterContext.setRecordBufferSize(chunkSize);
        storageWriterContext.getWriteJuffersWarmUpElement().setChunkTypeAsValid();

        WarmupElementBlocks warmupElementBlocks = new WarmupElementBlocks(chunkSize);
        buildLongBlocks(numberOfBlocks, recordsPerBlock)
                .forEach(warmupElementBlocks::add);

        assertThat(warmupElementBlocks.isReady()).isFalse();
        WarmResult warmResult = storageWriterService.appendWarmupElementBlocks(warmupElementBlocks, storageWriterContext);
        assertThat(warmResult.success()).isTrue();
        assertThat(warmResult.columnBlockIndex()).isEqualTo(numberOfBlocks);
        assertThat(warmResult.offset()).isEqualTo(0);
    }

    @Test
    public void testAppendWarmupElementBlocksReadyOnChunkSize()
    {
        int chunkSize = 10;
        int recordsPerBlock = 3;
        int expectedBlockIndex = chunkSize / recordsPerBlock;
        int expectedOffset = chunkSize % recordsPerBlock;

        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmUpElementWithDictionary(
                WarmColumnDataTestUtil.generateRecordData("col1", BIGINT),
                WarmUpType.WARM_UP_TYPE_DATA);

        StorageWriterSplitConfig storageWriterSplitConfig = startWarming("testAppendWarmupElementBlocksReadyOnChunkSize");
        StorageWriterContext storageWriterContext = txCreate(storageWriterSplitConfig, warmupElementWriteMetadata).storageWriterContext();
        storageWriterContext.setRecordBufferSize(chunkSize);
        storageWriterContext.getWriteJuffersWarmUpElement().setChunkTypeAsValid();

        WarmupElementBlocks warmupElementBlocks = new WarmupElementBlocks(chunkSize);
        buildLongBlocks(expectedBlockIndex + 1, recordsPerBlock)
                .forEach(warmupElementBlocks::add);

        assertThat(warmupElementBlocks.isReady()).isTrue();
        WarmResult warmResult = storageWriterService.appendWarmupElementBlocks(warmupElementBlocks, storageWriterContext);
        assertThat(warmResult.success()).isTrue();
        assertThat(warmResult.columnBlockIndex()).isEqualTo(expectedBlockIndex);
        assertThat(warmResult.offset()).isEqualTo(expectedOffset);
    }

    @Test
    public void testAppendWarmupElementBlocksNumberOfRecordsEqualsChunkSize()
    {
        int recordsPerBlock = 3;
        int blocksNumber = 5;
        int chunkSize = recordsPerBlock * blocksNumber;

        WarmupElementWriteMetadata warmupElementWriteMetadata = WarmColumnDataTestUtil.createWarmUpElementWithDictionary(
                WarmColumnDataTestUtil.generateRecordData("col1", BIGINT),
                WarmUpType.WARM_UP_TYPE_DATA);

        StorageWriterSplitConfig storageWriterSplitConfig = startWarming("testAppendWarmupElementBlocksNumberOfRecordsEqualsChunkSize");
        StorageWriterContext storageWriterContext = txCreate(storageWriterSplitConfig, warmupElementWriteMetadata).storageWriterContext();
        storageWriterContext.setRecordBufferSize(chunkSize);
        storageWriterContext.getWriteJuffersWarmUpElement().setChunkTypeAsValid();

        WarmupElementBlocks warmupElementBlocks = new WarmupElementBlocks(chunkSize);
        buildLongBlocks(blocksNumber, recordsPerBlock)
                .forEach(warmupElementBlocks::add);

        assertThat(warmupElementBlocks.isReady()).isTrue();
        WarmResult warmResult = storageWriterService.appendWarmupElementBlocks(warmupElementBlocks, storageWriterContext);
        assertThat(warmResult.success()).isTrue();
        assertThat(warmResult.columnBlockIndex()).isEqualTo(blocksNumber);
        assertThat(warmResult.offset()).isEqualTo(0);
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
        return storageWriterService.startWarming("nodeIdentifier",
                rowGroupFilePath + suffix,
                true,
                true);
    }

    private Page buildArrayType_VarcharPage(String[][] values)
    {
        ArrayBlockBuilder blockBuilder = new ArrayBlockBuilder(VARCHAR, null, 100);
        for (String[] stringArray : values) {
            BlockBuilder elementBlockBuilder = VARCHAR.createBlockBuilder(null, stringArray.length);
            for (String v : stringArray) {
                if (v == null) {
                    elementBlockBuilder.appendNull();
                }
                else {
                    VARCHAR.writeString(elementBlockBuilder, v);
                }
            }
            ArrayType arrayType = new ArrayType(VARCHAR);
            arrayType.writeObject(blockBuilder, elementBlockBuilder.build());
        }
        Block block = blockBuilder.build();

        return new Page(block);
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

    private Page buildArrayType_BigIntPage(long[][] values)
    {
        BlockBuilder blockBuilder = new ArrayBlockBuilder(BIGINT, null, values.length, values.length);
        writeBigIntValues(blockBuilder, values);
        Block block = blockBuilder.build();
        return new Page(block);
    }

    private void writeBigIntValues(BlockBuilder blockBuilder, long[][] values)
    {
        for (long[] longArray : values) {
            BlockBuilder elementBlockBuilder = BIGINT.createBlockBuilder(null, longArray.length);
            for (long v : longArray) {
                BIGINT.writeLong(elementBlockBuilder, v);
            }
            ArrayType arrayType = new ArrayType(BIGINT);
            arrayType.writeObject(blockBuilder, elementBlockBuilder.build());
        }
    }

    private Page buildArrayType_IntPage(int... values)
    {
        BlockBuilder blockBuilder = new ArrayBlockBuilder(IntegerType.INTEGER, null, values.length, values.length);
        writeIntValues(blockBuilder, values);
        Block block = blockBuilder.build();
        return new Page(block);
    }

    private void writeIntValues(BlockBuilder blockBuilder, int... values)
    {
        BlockBuilder elementBlockBuilder = IntegerType.INTEGER.createBlockBuilder(null, values.length);
        for (int v : values) {
            IntegerType.INTEGER.writeLong(elementBlockBuilder, v);
        }
        ArrayType arrayType = new ArrayType(IntegerType.INTEGER);
        arrayType.writeObject(blockBuilder, elementBlockBuilder.build());
    }

    private List<Block> buildLongBlocks(int numberOfBlocks, int valuesOnEachBlock)
    {
        List<Block> blocks = new ArrayList<>(numberOfBlocks);
        for (int i = 0; i < numberOfBlocks; i++) {
            LongArrayBlockBuilder block = new LongArrayBlockBuilder(null, valuesOnEachBlock);
            IntStream.range(0, valuesOnEachBlock).forEach(block::writeLong);
            LazyBlock lazyBlock = new LazyBlock(valuesOnEachBlock, block::build);
            blocks.add(lazyBlock);
        }
        return blocks;
    }
}
