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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.dictionary.DictionaryCacheService;
import io.trino.plugin.warp.dictionary.DictionaryException;
import io.trino.plugin.warp.dictionary.DictionaryWarmInfo;
import io.trino.plugin.warp.dictionary.WriteDictionary;
import io.trino.plugin.warp.dispatcher.WarmupElementWriteMetadata;
import io.trino.plugin.warp.dispatcher.model.DictionaryInfo;
import io.trino.plugin.warp.dispatcher.model.DictionaryKey;
import io.trino.plugin.warp.dispatcher.model.DictionaryState;
import io.trino.plugin.warp.dispatcher.model.WarmState;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.model.WarmUpElementState;
import io.trino.plugin.warp.dispatcher.warmup.warmers.WarmSinkResult;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.gen.stats.LuceneIndexerStats;
import io.trino.plugin.warp.juffer.BlockPosHolder;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.juffer.WarmUpElementAllocationParams;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.metrics.PrintMetricsTimerTask;
import io.trino.plugin.warp.storage.engine.ExceptionThrower;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.juffers.RecordBufferParams;
import io.trino.plugin.warp.storage.juffers.WriteJuffersWarmUpElement;
import io.trino.plugin.warp.storage.lucene.LuceneIndexer;
import io.trino.plugin.warp.storage.memory.ThreadArena;
import io.trino.plugin.warp.storage.memory.WorkerMemoryManager;
import io.trino.plugin.warp.storage.write.appenders.AppendResult;
import io.trino.plugin.warp.storage.write.appenders.BlockAppender;
import io.trino.plugin.warp.storage.write.appenders.BlockAppenderFactory;
import io.trino.plugin.warp.tools.util.Pair;
import io.trino.plugin.warp.type.TypeUtils;
import io.trino.plugin.warp.warmup.exceptions.WarmupException;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;
import java.lang.foreign.ValueLayout;
import java.util.Locale;
import java.util.Optional;

import static io.trino.plugin.warp.dictionary.DictionaryCacheService.DICTIONARY_REC_TYPE_CODE;
import static io.trino.plugin.warp.dictionary.DictionaryCacheService.DICTIONARY_REC_TYPE_LENGTH;
import static io.trino.plugin.warp.dispatcher.warmup.warmers.WarmupElementsCreator.getCurrentThreadWarmId;
import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_FD;
import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_FILE_HASH;
import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_FILE_MOD_TIME;
import static java.util.Objects.requireNonNull;

@Singleton
public class StorageWriterService
{
    private static final Logger logger = Logger.get(StorageWriterService.class);

    private final StorageEngine storageEngine;
    private final StorageEngineConstants storageEngineConstants;
    private final BufferAllocator bufferAllocator;
    private final DictionaryCacheService dictionaryCacheService;
    private final BlockAppenderFactory blockAppenderFactory;
    private final WarmupElementStatsService warmupElementStatsService;
    private final WorkerMemoryManager workerMemoryManager;
    private final PrintMetricsTimerTask metricsTimerTask;
    private final LuceneIndexerStats statsLuceneIndexer;
    private final NativeConfig nativeConfig;
    private final ShapingLoggerFactory shapingLoggerFactory;

    @Inject
    public StorageWriterService(
            StorageEngine storageEngine,
            StorageEngineConstants storageEngineConstants,
            BufferAllocator bufferAllocator,
            DictionaryCacheService dictionaryCacheService,
            MetricsManager metricsManager,
            PrintMetricsTimerTask metricsTimerTask,
            BlockAppenderFactory blockAppenderFactory,
            WarmupElementStatsService warmupElementStatsService,
            WorkerMemoryManager workerMemoryManager,
            NativeConfig nativeConfig,
            ShapingLoggerFactory shapingLoggerFactory)
    {
        this.storageEngine = requireNonNull(storageEngine);
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.bufferAllocator = requireNonNull(bufferAllocator);
        this.dictionaryCacheService = requireNonNull(dictionaryCacheService);
        this.blockAppenderFactory = requireNonNull(blockAppenderFactory);
        this.warmupElementStatsService = requireNonNull(warmupElementStatsService);
        this.workerMemoryManager = requireNonNull(workerMemoryManager);
        LuceneIndexerStats luceneIndexerStats = new LuceneIndexerStats();
        this.statsLuceneIndexer = metricsManager.registerMetric(luceneIndexerStats);
        this.metricsTimerTask = requireNonNull(metricsTimerTask);
        this.nativeConfig = requireNonNull(nativeConfig);
        this.shapingLoggerFactory = requireNonNull(shapingLoggerFactory);
    }

    public StorageWriterSplitConfig startWarming(
            String nodeIdentifier,
            String rowGroupFilePath,
            Boolean dictionaryEnabled,
            boolean allocateCommonWarmUpState)
    {
        /* allocate memory resources */
        ThreadArena arena = workerMemoryManager.getThreadArena();
        Optional<SegmentAllocator> warmMemoryAllocatorOpt = bufferAllocator.createWarmMemoryAllocator(arena, allocateCommonWarmUpState);
        if (!warmMemoryAllocatorOpt.isPresent()) {
            throw new RuntimeException("no memory available for warming");
        }
        SegmentAllocator warmMemoryAllocator = warmMemoryAllocatorOpt.get();
        Optional<WarmUpState> warmUpStateOpt = allocateCommonWarmUpState ? Optional.of(allocateWarmUpState(arena)) : Optional.empty();
        Optional<CompressionState> compressionStateOpt = allocateCommonWarmUpState ? Optional.of(allocateCompressionState(arena)) : Optional.empty();
        return new StorageWriterSplitConfig(
                nodeIdentifier,
                rowGroupFilePath,
                warmMemoryAllocator,
                bufferAllocator.allocateLoadSegment(warmMemoryAllocator),
                bufferAllocator.allocateLoadWriteBuffer(warmMemoryAllocator),
                bufferAllocator.allocateLoadContextAllocator(warmMemoryAllocator, allocateCommonWarmUpState),
                warmUpStateOpt,
                new RecordBufferParams(arena.allocate(RecordBufferParams.RECORD_BUFFER_PARAMS_LAYOUT.byteSize(), ValueLayout.JAVA_INT.byteSize())),
                compressionStateOpt,
                dictionaryEnabled,
                arena);
    }

    public void finishWarming(StorageWriterSplitConfig storageWriterSplitConfig)
    {
        /* all off-heap memory is freed by the aren once the object is closed */
        try {
            storageWriterSplitConfig.arena().close();
        }
        catch (Throwable t) {
            logger.error(t, "failed to close warming memory arena");
            throw new RuntimeException("ailed to close warming memory arena");
        }
    }

    WriteOpenResult open(
            long[] fileCookieParams,
            int startOffset,
            StorageWriterSplitConfig storageWriterSplitConfig,
            WarmupElementWriteMetadata warmupElementWriteMetadata)
    {
        Optional<LuceneIndexer> luceneIndexerOpt = Optional.empty();
        Optional<WriteDictionary> writeDictionaryOpt = Optional.empty();
        Pair<DictionaryKey, DictionaryState> dictionaryKeyAndState = dictionaryOpen(warmupElementWriteMetadata, storageWriterSplitConfig.nodeIdentifier(), storageWriterSplitConfig.dictionaryEnabled());
        DictionaryState dictionaryState = dictionaryKeyAndState.getValue();
        DictionaryKey dictionaryKey = dictionaryKeyAndState.getKey();
        WarmUpElementAllocationParams allocParams = bufferAllocator.calculateAllocationParams(warmupElementWriteMetadata, storageWriterSplitConfig.buff());

        // initialize file
        WarmUpElement warmUpElement = warmupElementWriteMetadata.warmUpElement();
        WarmUpElement.Builder warmupElementBuilder = WarmUpElement.builder(warmUpElement);
        warmupElementBuilder.startOffset(startOffset);
        // dictionary
        boolean hasDictionary = dictionaryState == DictionaryState.DICTIONARY_VALID;
        if (dictionaryState == DictionaryState.DICTIONARY_REJECTED) {
            warmupElementBuilder.dictionaryInfo(new DictionaryInfo(dictionaryKey, dictionaryState, 0, 0));
        }
        // initialize warm up state and file if needed
        WarmUpState warmUpState = storageWriterSplitConfig.warmUpStateOpt().orElseGet(() -> allocateWarmUpState(storageWriterSplitConfig.arena()));
        CompressionState compressionState = storageWriterSplitConfig.compressionStateOpt().orElseGet(() -> allocateCompressionState(storageWriterSplitConfig.arena()));
        storageWeOpen(
                warmUpElement,
                hasDictionary,
                warmUpState,
                fileCookieParams,
                storageWriterSplitConfig.contextAllocator().allocate(warmUpElement.getWarmUpContextSize(), Integer.BYTES),
                startOffset,
                storageWriterSplitConfig.writeBuff().address(),
                allocParams);

        // set up buffers
        byte warmId = getCurrentThreadWarmId();
        WriteJuffersWarmUpElement writeJuffersWarmUpElement = createWriteJuffers(
                storageWriterSplitConfig.recordBufferParams(),
                warmUpState,
                hasDictionary,
                allocParams,
                compressionState,
                warmId,
                storageWriterSplitConfig.arena());
        if (hasDictionary) {
            WriteDictionary writeDictionary = dictionaryCacheService.computeWriteIfAbsent(dictionaryKey, warmUpElement.getRecTypeCode());
            dictionaryKey = writeDictionary.getDictionaryKey(); // in order to be aligned with createdTimestamp
            writeDictionaryOpt = Optional.of(writeDictionary);
        }
        // set up dictionary
        DictionaryWarmInfo dictionaryWarmInfo = new DictionaryWarmInfo(dictionaryState, dictionaryKey);

        if (warmUpElement.getWarmUpType() == WarmUpType.WARM_UP_TYPE_LUCENE) {
            // initialize lucene
            LuceneIndexer luceneIndexer = new LuceneIndexer(
                    storageEngineConstants,
                    shapingLoggerFactory,
                    storageWriterSplitConfig.rowGroupFilePath(),
                    statsLuceneIndexer);
            luceneIndexerOpt = Optional.of(luceneIndexer);
        }

        BlockAppender blockAppender = blockAppenderFactory.createBlockAppender(
                warmUpElement,
                warmupElementWriteMetadata.type(),
                writeJuffersWarmUpElement,
                luceneIndexerOpt);

        StorageWriterContext storageWriterContext = new StorageWriterContext(
                warmupElementWriteMetadata,
                warmupElementBuilder,
                writeJuffersWarmUpElement,
                dictionaryWarmInfo,
                warmUpState,
                blockAppender,
                writeDictionaryOpt,
                luceneIndexerOpt,
                warmId);
        return new WriteOpenResult(storageWriterContext, dictionaryWarmInfo);
    }

    private WarmUpState allocateWarmUpState(ThreadArena arena)
    {
        final int storageBufferMetadaSize = nativeConfig.getLimitNumIosInParallel() * nativeConfig.getMaxIOMetadataSize() / 100; // we take 1 percentage of the read size for write
        return new WarmUpState(arena.allocate(WarmUpState.WARMUP_STATE_LAYOUT.byteSize() + storageBufferMetadaSize, ValueLayout.JAVA_INT.byteSize()));
    }

    private CompressionState allocateCompressionState(ThreadArena arena)
    {
        return new CompressionState(arena.allocate(CompressionState.COMPRESSION_STATE_LAYOUT.byteSize(), ValueLayout.JAVA_INT.byteSize()));
    }

    private WriteJuffersWarmUpElement createWriteJuffers(
            RecordBufferParams recordBufferParams,
            WarmUpState warmUpState,
            boolean dictionaryValid,
            WarmUpElementAllocationParams allocParams,
            CompressionState compressionState,
            byte warmId,
            ThreadArena arena)
    {
        WriteJuffersWarmUpElement juffersWE = new WriteJuffersWarmUpElement(
                storageEngine,
                storageEngineConstants,
                bufferAllocator,
                recordBufferParams,
                warmUpState,
                allocParams,
                compressionState,
                warmId,
                arena);
        juffersWE.createBuffers(dictionaryValid);
        return juffersWE;
    }

    private Pair<DictionaryKey, DictionaryState> dictionaryOpen(
            WarmupElementWriteMetadata warmupElementWriteMetadata,
            String nodeIdentifier,
            Boolean dictionaryEnabled)
    {
        DictionaryKey dictionaryKey;
        WarmUpElement warmUpElement = warmupElementWriteMetadata.warmUpElement();
        if (warmUpElement.getDictionaryInfo() != null) {
            dictionaryKey = warmUpElement.getDictionaryInfo().dictionaryKey();
        }
        else {
            long createdTimestamp = dictionaryCacheService.getLastCreatedTimestamp(warmupElementWriteMetadata.schemaTableColumn(), nodeIdentifier);
            dictionaryKey = new DictionaryKey(warmupElementWriteMetadata.schemaTableColumn(), nodeIdentifier, createdTimestamp);
        }
        DictionaryState dictionaryState;
        if (!warmupElementWriteMetadata.fitForDictionary()) {
            dictionaryState = DictionaryState.DICTIONARY_NOT_EXIST;
        }
        else {
            dictionaryState = dictionaryCacheService.calculateDictionaryStateForWrite(dictionaryKey, warmUpElement, dictionaryEnabled);
        }
        return Pair.of(dictionaryKey, dictionaryState);
    }

    private void storageWeOpen(
            WarmUpElement warmUpElement,
            boolean hasDictionary,
            WarmUpState warmUpState,
            long[] fileCookieParams,
            MemorySegment context,
            int startOffset,
            long writeBufAddr,
            WarmUpElementAllocationParams allocParams)
    {
        // file set parameters, set per warmup element
        warmUpState.setFileCookie(
                (int) fileCookieParams[FILE_COOKIE_PARAMS_FD.ordinal()],
                (long) fileCookieParams[FILE_COOKIE_PARAMS_FILE_HASH.ordinal()],
                (long) fileCookieParams[FILE_COOKIE_PARAMS_FILE_MOD_TIME.ordinal()]);
        // warm up element attributes
        warmUpState.setWarmUpElementAtt(
                hasDictionary ? DICTIONARY_REC_TYPE_CODE : TypeUtils.nativeRecTypeCode(warmUpElement.getRecTypeCode()),
                hasDictionary ? DICTIONARY_REC_TYPE_LENGTH : warmUpElement.getRecTypeLength(),
                warmUpElement.getWarmUpType());
        // juffers
        bufferAllocator.setWarmBuffers(allocParams, warmUpState);
        // other properties
        warmUpState.setStartOffset(startOffset);
        warmUpState.setWriteBuff(writeBufAddr);
        warmUpState.resetWarmEvents();
        warmUpState.setCloseChunk(false); // keep it false as default
        storageEngine.warmupElementOpen(warmUpState.getMemory(), context);
        warmUpState.verifyWarmUpSuccess();
    }

    WarmSinkResult close(int totalRecords, StorageWriterSplitConfig storageWriterSplitConfig, StorageWriterContext storageWriterContext)
    {
        Optional<WarmUpCloseResult> warmUpCloseResultOpt = cleanup(false, false, storageWriterContext);

        WarmUpElement.Builder warmupElementBuilder = storageWriterContext.getWarmupElementBuilder();
        if (!storageWriterContext.weSuccess() || !warmUpCloseResultOpt.isPresent()) {
            return new WarmSinkResult(warmupElementBuilder.build(), 0);
        }

        WarmupElementWriteMetadata warmupElementWriteMetadata = storageWriterContext.getWarmupElementWriteMetadata();
        WarmupElementStats closedStats = warmupElementStatsService.getFinalStats(
                warmupElementWriteMetadata.type(),
                storageWriterContext.getWarmupElementStatsBuilder().build(),
                warmupElementWriteMetadata.warmUpElement().getRecTypeCode(),
                warmupElementWriteMetadata.warmUpElement().getWarmUpType());

        WarmUpCloseResult warmUpCloseResult = warmUpCloseResultOpt.get();
        warmupElementBuilder.state(WarmUpElementState.VALID)
                .warmState(WarmState.HOT)
                .queryOffset(warmUpCloseResult.queryOffset())
                .queryReadSize(warmUpCloseResult.querySize())
                .warmEvents(storageWriterContext.getWarmUpState().getWarmEvents())
                .warmId((int) storageWriterContext.getWarmId())
                .totalRecords(totalRecords)
                .warmupElementStats(closedStats);

        // update record type length if needed
        final int actualRecTypeLength = storageWriterContext.getWriteJuffersWarmUpElement().getActualRecTypeLength();
        if (actualRecTypeLength > 0) {
            final int recTypeLength = warmupElementWriteMetadata.warmUpElement().getRecTypeLength();
            if (actualRecTypeLength < recTypeLength) {
                warmupElementBuilder.recTypeLength(actualRecTypeLength);
            }
        }
        else if (actualRecTypeLength == 0) {
            warmupElementBuilder.recTypeLength(1); // 1 is the minimum allowed length
        }

        int offset = warmUpCloseResult.endOffset();
        if (offset <= 0) {
            logger.error("offset is zero warmupElementWriteMetadata %s storageWriterSplitConfig %s", warmupElementWriteMetadata, storageWriterSplitConfig);
            // Native failed to write
            updateToFailedState(warmupElementBuilder, warmupElementWriteMetadata);
            storageWriterContext.setFailed();
        }
        // attach dictionary if needed
        if (storageWriterContext.weSuccess() && storageWriterContext.getWriteDictionary().isPresent()) {
            int dictionarySize = 0;

            WriteDictionary writeDictionary = storageWriterContext.getWriteDictionary().get();
            try {
                dictionarySize = dictionaryCacheService.writeDictionary(
                        writeDictionary.getDictionaryKey(),
                        warmupElementWriteMetadata.warmUpElement().getRecTypeCode(),
                        offset,
                        storageWriterSplitConfig.rowGroupFilePath());
            }
            catch (Exception e) {
                storageWriterContext.setFailed();
                updateToFailedState(warmupElementBuilder, warmupElementWriteMetadata);
            }
            logger.debug(
                    "close fileOffsetsEnd (= dictionaryOffset) %d dictionarySize %d",
                    offset,
                    dictionarySize);

            if (dictionarySize != 0) {
                DictionaryInfo dictionaryInfo = new DictionaryInfo(
                        writeDictionary.getDictionaryKey(),
                        storageWriterContext.getDictionaryWarmInfo().dictionaryState(),
                        writeDictionary.getRecTypeLength(),
                        offset);
                warmupElementBuilder.dictionaryInfo(dictionaryInfo)
                        .usedDictionarySize(writeDictionary.getWriteSize());
                offset += dictionarySize;
            }
        }
        // write lucene info if needed
        if (storageWriterContext.weSuccess() && storageWriterContext.getLuceneIndexer().isPresent()) {
            int luceneSize = 0;

            try {
                luceneSize = storageWriterContext.getLuceneIndexer().get().saveLuceneIndexState(offset);
            }
            catch (Exception e) {
                storageWriterContext.setFailed();
                updateToFailedState(warmupElementBuilder, warmupElementWriteMetadata);
            }
            logger.debug("close fileOffsetsEnd (= luceneOffset) %d luceneSize %d", offset, luceneSize);

            if (luceneSize != 0) {
                warmupElementBuilder.matchOffset(offset)
                        .matchReadSize(luceneSize);
                offset += luceneSize;
            }
        }
        if (storageWriterContext.weSuccess()) {
            warmupElementBuilder.endOffset(offset);
        }
        if (offset == 0 && storageWriterContext.weSuccess()) {
            updateToFailedState(storageWriterContext.getWarmupElementBuilder(), warmupElementWriteMetadata);
        }
        return new WarmSinkResult(warmupElementBuilder.build(), offset);
    }

    // bad path cleanup of resources and release the storage engine tx
    WarmUpElement abort(boolean nativeThrowed, StorageWriterContext storageWriterContext, StorageWriterSplitConfig storageWriterSplitConfig)
    {
        storageWriterContext.getLuceneIndexer().ifPresent(LuceneIndexer::abort);
        cleanup(true, nativeThrowed, storageWriterContext);
        if (storageWriterContext.weSuccess()) {
            updateToFailedState(storageWriterContext.getWarmupElementBuilder(), storageWriterContext.getWarmupElementWriteMetadata());
            storageWriterContext.setFailed();
        }

        WarmUpElement abortedWarmupElement = storageWriterContext.getWarmupElementBuilder().build();
        if (nativeThrowed) {
            logger.error("warm failed path %s native throwed on element %s", storageWriterSplitConfig.rowGroupFilePath(), abortedWarmupElement);
            metricsTimerTask.print(false, Optional.of(String.format(Locale.US, "warm failed path %s native throwed on element %s", storageWriterSplitConfig.rowGroupFilePath(), abortedWarmupElement)));
        }
        return abortedWarmupElement;
    }

    boolean appendPage(SourcePage page, StorageWriterContext storageWriterContext)
    {
        WarmupElementWriteMetadata warmupElementWriteMetadata = storageWriterContext.getWarmupElementWriteMetadata();
        Block block = page.getBlock(warmupElementWriteMetadata.connectorBlockIndex());
        int totalRecords = block.getPositionCount();

        int currentRecordNumber = 0;
        while (storageWriterContext.weSuccess() && currentRecordNumber < totalRecords) {
            recycleBuffers(storageWriterContext);

            int maxRecordsToAdd = Math.min(storageWriterContext.getRemainingBufferSize(), totalRecords - currentRecordNumber);
            BlockPosHolder blockPosHolder = new BlockPosHolder(block, warmupElementWriteMetadata.type(), currentRecordNumber, maxRecordsToAdd);

            appendToBuffer(storageWriterContext, blockPosHolder);
            storageWriterContext.incRecordBufferPos(blockPosHolder.getPos());
            currentRecordNumber += blockPosHolder.getPos();
        }
        return storageWriterContext.weSuccess();
    }

    private Optional<WarmUpCloseResult> cleanup(boolean aborted, boolean nativeThrowed, StorageWriterContext storageWriterContext)
    {
        Optional<WarmUpCloseResult> warmUpCloseResultOpt = Optional.empty();
        if (storageWriterContext.getIsCleanupDone().compareAndSet(null, aborted)) {
            try {
                if (!aborted) {
                    try {
                        if (storageWriterContext.getRecordBufferPos() > 0) {
                            flushRecordBuffer(storageWriterContext);
                        }
                    }
                    catch (TrinoException te) {
                        nativeThrowed |= ExceptionThrower.isNativeException(te);
                        aborted = true;
                    }
                }
                if (!nativeThrowed) {
                    warmUpCloseResultOpt = weClose(storageWriterContext);
                    if (!warmUpCloseResultOpt.isPresent()) {
                        aborted = true;
                    }
                }
                if (aborted) {
                    storageWriterContext.setFailed();
                    updateToFailedState(storageWriterContext.getWarmupElementBuilder(), storageWriterContext.getWarmupElementWriteMetadata());
                }
            }
            catch (Exception e) {
                logger.error(e, "abort tx wes %s", storageWriterContext.getWarmupElementWriteMetadata());
                throw e;
            }
            finally {
                storageWriterContext.getIsCleanupDone().getAndSet(true);
            }
        }
        return warmUpCloseResultOpt;
    }

    void updateToFailedState(WarmUpElement.Builder warmupElementBuilder, WarmupElementWriteMetadata warmupElementWriteMetadata)
    {
        warmupElementBuilder.state(new WarmUpElementState(
                        WarmUpElementState.State.FAILED_TEMPORARILY,
                        warmupElementWriteMetadata.warmUpElement().getState().temporaryFailureCount(),
                        System.currentTimeMillis()))
                .startOffset(-1)
                .queryOffset(-1)
                .queryReadSize(-1)
                .endOffset(-1)
                .warmState(WarmState.COLD);
    }

    /**
     * Flushes the record buffer to storage.
     */
    private void flushRecordBuffer(StorageWriterContext storageWriterContext)
    {
        WriteJuffersWarmUpElement writeJuffersWarmUpElement = storageWriterContext.getWriteJuffersWarmUpElement();
        if (storageWriterContext.getLuceneIndexer().isPresent()) {
            final int startOffset = storageWriterContext.getWarmUpState().getStartOffset();
            final int endOffset = storageWriterContext.getLuceneIndexer().get().closeAndSaveLuceneIndex(startOffset);
            storageWriterContext.getWarmUpState().setStartOffset(endOffset);
        }

        if (storageWriterContext.weSuccess()) {
            storageWriterContext.getWarmUpState().setCloseChunk(true);
            writeJuffersWarmUpElement.commitWE(storageWriterContext.getRecordBufferPos());
            storageWriterContext.getWarmUpState().setCloseChunk(false); // keep it false as default for all other calls
        }
        storageWriterContext.resetRecords();
    }

    private Optional<WarmUpCloseResult> weClose(StorageWriterContext storageWriterContext)
    {
        Optional<WarmUpCloseResult> warmUpCloseResultOpt = Optional.empty();

        if (!storageWriterContext.isWeClosed()) {
            WarmUpState warmUpState = storageWriterContext.getWarmUpState();
            int queryOffset = warmUpState.getStartOffset();
            storageWriterContext.getWriteJuffersWarmUpElement().writeChunkListToJuffer(queryOffset);

            // if current chunk is still opened it means there was an exception and we warm up element is aborted
            WriteJuffersWarmUpElement writeJuffersWarmUpElement = storageWriterContext.getWriteJuffersWarmUpElement();
            boolean currentChunkIsOpened = writeJuffersWarmUpElement.closeCurrentChunk();
            int numChunks = writeJuffersWarmUpElement.getNumChunks();
            if (!currentChunkIsOpened && (numChunks > 0)) { // if current chunk is opened it means we got a native exception in the middle
                warmUpState.setNumChunks((short) numChunks);
                writeJuffersWarmUpElement.getAndVerifyNumChunkPages();
                int querySize = storageEngine.warmupElementClose(warmUpState.getMemory());
                warmUpCloseResultOpt = Optional.of(new WarmUpCloseResult(queryOffset, querySize, warmUpState.getStartOffset()));
            }
            storageWriterContext.setWeClosed();
        }
        return warmUpCloseResultOpt;
    }

    /**
     * Initializes or recycles the buffers if full. If already initialized and not full, won't do anything.
     *
     * @return true if flush occurred
     */
    private boolean recycleBuffers(StorageWriterContext storageWriterContext)
    {
        boolean flushed = false;
        if (storageWriterContext.isRecordBufferFull()) {
            flushRecordBuffer(storageWriterContext);
            flushed = true;
        }
        else if (storageWriterContext.getRecordBufferSize() > 0) {
            return false;
        }

        storageWriterContext.resetRecordBufferPos();
        storageWriterContext.setRecordBufferSize(1 << storageEngineConstants.getChunkSizeShift());

        storageWriterContext.getWriteJuffersWarmUpElement().resetAllBuffers();
        if (storageWriterContext.getLuceneIndexer().isPresent()) {
            storageWriterContext.getLuceneIndexer().get().resetLuceneIndex();
        }
        return flushed;
    }

    /**
     * Should be called sequentially with each col and its block
     * Returns if at least one buffer flush has occurred
     */
    private void appendToBuffer(StorageWriterContext storageWriterContext, BlockPosHolder blockPos)
    {
        WarmupElementWriteMetadata warmupElementWriteMetadata = storageWriterContext.getWarmupElementWriteMetadata();
        WarmUpElement warmUpElement = warmupElementWriteMetadata.warmUpElement();
        try {
            AppendResult appendResult = storageWriterContext.getBlockAppender().append(
                    storageWriterContext.getRecordBufferPos(),
                    blockPos,
                    storageWriterContext.getWriteDictionary(),
                    warmUpElement,
                    storageWriterContext.getWarmupElementStatsBuilder());
            storageWriterContext.getWriteJuffersWarmUpElement().increaseNullsCount(appendResult.nullsCount());
        }
        catch (WarmupException e) {
            storageWriterContext.setFailed();
            logger.debug("failed appending block to WE %s exception %s", warmupElementWriteMetadata, e);

            WarmUpElement.Builder warmupElementBuilder = WarmUpElement.builder(warmUpElement);
            if (e instanceof DictionaryException dictionaryMaxException) {
                DictionaryInfo dictionaryInfo = new DictionaryInfo(
                        dictionaryMaxException.getDictionaryKey(),
                        dictionaryMaxException.getDictionaryState(),
                        0, // dataValuesRecTypeLength
                        DictionaryInfo.NO_OFFSET);
                warmupElementBuilder.dictionaryInfo(dictionaryInfo);
                dictionaryCacheService.updateOnFailedWrite(dictionaryMaxException.getDictionaryKey());
            }
            warmupElementBuilder.state(new WarmUpElementState(e.getState(), 0, System.currentTimeMillis()));
            storageWriterContext.setWarmupElementBuilder(warmupElementBuilder);
        }
        catch (Exception e) {
            storageWriterContext.setFailed();
            logger.debug("failed appending block to WE %s exception %s", warmupElementWriteMetadata, e);
            updateToFailedState(storageWriterContext.getWarmupElementBuilder(), warmupElementWriteMetadata);
            throw e;
        }
    }

    private record WarmUpCloseResult(int queryOffset, int querySize, int endOffset) {}
}
