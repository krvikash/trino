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
import io.trino.plugin.warp.dictionary.DictionaryCacheService;
import io.trino.plugin.warp.dictionary.DictionaryMaxException;
import io.trino.plugin.warp.dictionary.DictionaryWarmInfo;
import io.trino.plugin.warp.dictionary.WriteDictionary;
import io.trino.plugin.warp.dispatcher.WarmupElementWriteMetadata;
import io.trino.plugin.warp.dispatcher.cache.WarmupElementBlocks;
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
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.metrics.PrintMetricsTimerTask;
import io.trino.plugin.warp.storage.engine.ExceptionThrower;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.juffers.WriteJuffersWarmUpElement;
import io.trino.plugin.warp.storage.lucene.LuceneIndexer;
import io.trino.plugin.warp.storage.write.appenders.AppendResult;
import io.trino.plugin.warp.storage.write.appenders.BlockAppender;
import io.trino.plugin.warp.storage.write.appenders.BlockAppenderFactory;
import io.trino.plugin.warp.tools.util.Pair;
import io.trino.plugin.warp.type.TypeUtils;
import io.trino.plugin.warp.warmup.exceptions.WarmupException;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentAllocator;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.warp.dictionary.DictionaryCacheService.DICTIONARY_REC_TYPE_CODE_NUM;
import static io.trino.plugin.warp.dictionary.DictionaryCacheService.DICTIONARY_REC_TYPE_LENGTH;
import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_START_OFFSET;
import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_WARM_EVENTS;
import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_WARM_ID;
import static java.util.Objects.requireNonNull;

@Singleton
public class StorageWriterService
{
    private static final Logger logger = Logger.get(StorageWriterService.class);
    private static final String LUCENE_STATS_GROUP_NAME = "lucene_index";

    private final StorageEngine storageEngine;
    private final StorageEngineConstants storageEngineConstants;
    private final BufferAllocator bufferAllocator;
    private final DictionaryCacheService dictionaryCacheService;
    private final BlockAppenderFactory blockAppenderFactory;
    private final WarmupElementStatsService warmupElementStatsService;
    private final PrintMetricsTimerTask metricsTimerTask;
    private final LuceneIndexerStats statsLuceneIndexer;

    enum WeProperties
    {
        WE_PROPERTIES_QUERY_OFFSET,
        WE_PROPERTIES_QUERY_READ_SIZE,
        WE_PROPERTIES_END_OFFSET // MUST BE LAST
    }

    @Inject
    public StorageWriterService(StorageEngine storageEngine,
            StorageEngineConstants storageEngineConstants,
            BufferAllocator bufferAllocator,
            DictionaryCacheService dictionaryCacheService,
            MetricsManager metricsManager,
            PrintMetricsTimerTask metricsTimerTask,
            BlockAppenderFactory blockAppenderFactory,
            WarmupElementStatsService warmupElementStatsService)
    {
        this.storageEngine = requireNonNull(storageEngine);
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.bufferAllocator = requireNonNull(bufferAllocator);
        this.dictionaryCacheService = requireNonNull(dictionaryCacheService);
        this.blockAppenderFactory = requireNonNull(blockAppenderFactory);
        this.warmupElementStatsService = requireNonNull(warmupElementStatsService);
        LuceneIndexerStats luceneIndexerStats = new LuceneIndexerStats(LUCENE_STATS_GROUP_NAME, "0");
        this.statsLuceneIndexer = metricsManager.registerMetric(luceneIndexerStats);
        this.metricsTimerTask = requireNonNull(metricsTimerTask);
    }

    public StorageWriterSplitConfig startWarming(String nodeIdentifier,
            String rowGroupFilePath,
            Boolean dictionaryEnabled)
    {
        MemorySegment buff = bufferAllocator.allocateLoadSegment();
        MemorySegment writeBuff = bufferAllocator.allocateLoadWriteBuffer();
        MemorySegment contextBuff = bufferAllocator.allocateLoadContextBuffer();
        SegmentAllocator contextAllocator = SegmentAllocator.slicingAllocator(contextBuff);
        return new StorageWriterSplitConfig(nodeIdentifier, rowGroupFilePath, buff, writeBuff, contextBuff, contextAllocator, dictionaryEnabled);
    }

    public void finishWarming(StorageWriterSplitConfig storageWriterSplitConfig)
    {
        bufferAllocator.freeLoadContextBuffer(storageWriterSplitConfig.contextBuff());
        bufferAllocator.freeLoadWriteBuffer(storageWriterSplitConfig.writeBuff());
        bufferAllocator.freeLoadSegment(storageWriterSplitConfig.buff());
    }

    StorageWriterContext open(long[] fileCookieParams,
            StorageWriterSplitConfig storageWriterSplitConfig,
            WarmupElementWriteMetadata warmupElementWriteMetadata,
            List<DictionaryWarmInfo> outDictionaryWarmInfos)
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
        warmupElementBuilder.startOffset((int) fileCookieParams[FILE_COOKIE_PARAMS_START_OFFSET.ordinal()]);
        // open storage engine WE
        boolean hasDictionary = dictionaryState == DictionaryState.DICTIONARY_VALID;
        if (dictionaryState == DictionaryState.DICTIONARY_REJECTED) {
            warmupElementBuilder.dictionaryInfo(new DictionaryInfo(dictionaryKey, dictionaryState, 0, 0));
        }
        StorageOpenResult storageOpenResult = storageWeOpen(warmUpElement,
                hasDictionary,
                storageWriterSplitConfig.contextAllocator().allocate(warmUpElement.getWarmUpContextSize(), Integer.BYTES).address(),
                allocParams);

        // set up buffers
        byte[] compressionStats = new byte[35];
        WriteJuffersWarmUpElement writeJuffersWarmUpElement = getWriteJuffersWarmUpElement(storageOpenResult, hasDictionary, allocParams, fileCookieParams, compressionStats);
        if (hasDictionary) {
            WriteDictionary writeDictionary = dictionaryCacheService.computeWriteIfAbsent(dictionaryKey, warmUpElement.getRecTypeCode());
            dictionaryKey = writeDictionary.getDictionaryKey(); //in order to be aligned with createdTimestamp
            writeDictionaryOpt = Optional.of(writeDictionary);
        }
        //set up dictionary
        DictionaryWarmInfo dictionaryWarmInfo = new DictionaryWarmInfo(dictionaryState, dictionaryKey);
        outDictionaryWarmInfos.add(dictionaryWarmInfo);

        if (warmUpElement.getWarmUpType() == WarmUpType.WARM_UP_TYPE_LUCENE) {
            // initialize lucene
            LuceneIndexer luceneIndexer = new LuceneIndexer(storageEngineConstants,
                    storageWriterSplitConfig.rowGroupFilePath(),
                    statsLuceneIndexer);
            luceneIndexerOpt = Optional.of(luceneIndexer);
        }

        BlockAppender blockAppender = blockAppenderFactory.createBlockAppender(warmUpElement,
                warmupElementWriteMetadata.type(),
                writeJuffersWarmUpElement,
                luceneIndexerOpt);

        return new StorageWriterContext(warmupElementWriteMetadata,
                warmupElementBuilder,
                writeJuffersWarmUpElement,
                dictionaryWarmInfo,
                storageOpenResult.weCookie(),
                storageOpenResult.recTypeCode(),
                storageOpenResult.recTypeLength(),
                storageOpenResult.warmUpType(),
                fileCookieParams,
                storageOpenResult.buffAddresses(),
                compressionStats,
                blockAppender,
                writeDictionaryOpt,
                luceneIndexerOpt);
    }

    private WriteJuffersWarmUpElement getWriteJuffersWarmUpElement(StorageOpenResult storageOpenResult,
            boolean dictionaryValid,
            WarmUpElementAllocationParams allocParams,
            long[] fileCookieParams,
            byte[] compressionStats)
    {
        WriteJuffersWarmUpElement juffersWE = new WriteJuffersWarmUpElement(storageEngine,
                storageEngineConstants,
                bufferAllocator,
                storageOpenResult.buffs(),
                storageOpenResult.weCookie(),
                storageOpenResult.recTypeCode(),
                storageOpenResult.recTypeLength(),
                storageOpenResult.warmUpType(),
                allocParams,
                fileCookieParams,
                storageOpenResult.buffAddresses(),
                compressionStats);
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
        DictionaryState dictionaryState = dictionaryCacheService.calculateDictionaryStateForWrite(dictionaryKey, warmUpElement, dictionaryEnabled);
        return Pair.of(dictionaryKey, dictionaryState);
    }

    private StorageOpenResult storageWeOpen(WarmUpElement warmUpElement,
            boolean hasDictionary,
            long context,
            WarmUpElementAllocationParams allocParams)
    {
        // initialize warm up element attributes
        int recTypeCode = hasDictionary ? DICTIONARY_REC_TYPE_CODE_NUM : TypeUtils.nativeRecTypeCode(warmUpElement.getRecTypeCode());
        int recTypeLength = hasDictionary ? DICTIONARY_REC_TYPE_LENGTH : warmUpElement.getRecTypeLength();
        int warmUpType = warmUpElement.getWarmUpType().ordinal();

        MemorySegment[] buffs = bufferAllocator.getWarmBuffers(allocParams);
        long[] buffAddresses = new long[buffs.length];
        for (int i = 0; i < buffs.length; i++) {
            MemorySegment buff = buffs[i];
            if (buff != null) {
                buffAddresses[i] = buffs[i].address();
            }
        }

        // open storage engine WE
        long weCookie = storageEngine.warmupElementOpen(context,
                recTypeCode,
                recTypeLength,
                warmUpType);
        return new StorageOpenResult(buffs, weCookie, recTypeCode, recTypeLength, warmUpType, buffAddresses);
    }

    WarmSinkResult close(int totalRecords, StorageWriterSplitConfig storageWriterSplitConfig, StorageWriterContext storageWriterContext)
    {
        int[] outFileParams = cleanup(false, false, storageWriterContext);

        WarmUpElement.Builder warmupElementBuilder = storageWriterContext.getWarmupElementBuilder();
        if (!storageWriterContext.weSuccess() || outFileParams == null) {
            return new WarmSinkResult(warmupElementBuilder.build(), 0);
        }

        WarmupElementWriteMetadata warmupElementWriteMetadata = storageWriterContext.getWarmupElementWriteMetadata();
        WarmupElementStats closedStats = warmupElementStatsService.getFinalStats(
                warmupElementWriteMetadata.type(),
                storageWriterContext.getWarmupElementStatsBuilder().build(),
                warmupElementWriteMetadata.warmUpElement().getRecTypeCode(),
                warmupElementWriteMetadata.warmUpElement().getWarmUpType());

        long[] fileCookieParams = storageWriterContext.getFileCookieParams();
        warmupElementBuilder.state(WarmUpElementState.VALID)
                .warmState(WarmState.HOT)
                .queryOffset(outFileParams[WeProperties.WE_PROPERTIES_QUERY_OFFSET.ordinal()])
                .queryReadSize(outFileParams[WeProperties.WE_PROPERTIES_QUERY_READ_SIZE.ordinal()])
                .warmEvents((int) fileCookieParams[FILE_COOKIE_PARAMS_WARM_EVENTS.ordinal()])
                .warmId((int) fileCookieParams[FILE_COOKIE_PARAMS_WARM_ID.ordinal()])
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

        int offset = outFileParams[WeProperties.WE_PROPERTIES_END_OFFSET.ordinal()];
        if (offset == 0) {
            logger.error("offset 0 warmupElementWriteMetadata=%s, storageWriterSplitConfig=%s", warmupElementWriteMetadata, storageWriterSplitConfig);
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
            logger.debug("close fileOffsetsEnd (= dictionaryOffset) %d dictionarySize %d",
                    offset, dictionarySize);

            if (dictionarySize != 0) {
                DictionaryInfo dictionaryInfo = new DictionaryInfo(writeDictionary.getDictionaryKey(),
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
            metricsTimerTask.print(false);
        }
        return abortedWarmupElement;
    }

    boolean appendPage(Page page, StorageWriterContext storageWriterContext)
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

    WarmResult appendWarmupElementBlocks(WarmupElementBlocks warmupElementBlocks, StorageWriterContext storageWriterContext)
    {
        // isReady will be false supposedly in the last iteration (after Trino passed all the pages)
        // But the calculation is heuristic, and even when warmupElementBlocks is considered not ready,
        // it might actually contain enough data to fill the buffer.
        // In this case, we want to write all the data without stopping after one iteration
        boolean stopAfterOneChunk = warmupElementBlocks.isReady();
        int blockIndex = 0;
        boolean flushed = false;
        int currentRecordNumber = warmupElementBlocks.getStartOffsetInFirstBlock();
        for (; blockIndex < warmupElementBlocks.getSize() && !(stopAfterOneChunk && flushed) && storageWriterContext.weSuccess(); blockIndex++) {
            Block block = warmupElementBlocks.get(blockIndex);
            if (blockIndex > 0) {
                currentRecordNumber = 0;
            }
            int blockRows = block.getPositionCount();
            while (storageWriterContext.weSuccess() && currentRecordNumber < blockRows) {
                flushed = recycleBuffers(storageWriterContext);
                if (stopAfterOneChunk && flushed) {
                    break;
                }
                int maxRecordsToAdd = Math.min(storageWriterContext.getRemainingBufferSize(), block.getPositionCount() - currentRecordNumber);
                Type type = storageWriterContext.getWarmupElementWriteMetadata().type();
                BlockPosHolder blockPosHolder = new BlockPosHolder(block, type, currentRecordNumber, maxRecordsToAdd);

                appendToBuffer(storageWriterContext, blockPosHolder);
                storageWriterContext.incRecordBufferPos(blockPosHolder.getPos());
                currentRecordNumber += blockPosHolder.getPos();
            }
        }

        if (storageWriterContext.weSuccess() && !(stopAfterOneChunk && flushed)) {
            // Note that we don't add the amount of flushed records to 'currentRecordNumber' because blockPosHolder.getPos() already counted them
            flushAfterAppendingBlocks(warmupElementBlocks, storageWriterContext);
        }

        if (currentRecordNumber == warmupElementBlocks.get(blockIndex - 1).getPositionCount()) {
            // If the block was already read in full - point on the next block
            currentRecordNumber = 0;
        }
        else {
            // the for loop increased blockIndex and then existed, this is to point on the current block
            blockIndex--;
        }

        return new WarmResult(storageWriterContext.weSuccess(), blockIndex, currentRecordNumber);
    }

    private void flushAfterAppendingBlocks(WarmupElementBlocks warmupElementBlocks, StorageWriterContext storageWriterContext)
    {
        if (storageWriterContext.getRecordBufferPos() == 0) {
            // no data was written
            return;
        }

        if (storageWriterContext.isRecordBufferFull() ||  // for the case that we filled the buffer on the last iteration and exited because recycling
                !warmupElementBlocks.isReady()) { // When isReady() == false, we have to flush without waiting for cleanup() to do the job, because until then, the data on the buffers might get overwritten with data of another WarmUpElement
            flushRecordBuffer(storageWriterContext);
            return;
        }

        // Note that we have to throw an exception \ handle this case somehow, otherwise we'll report in WarmResult that we wrote some data,
        // while it wasn't actually flushed (and therefor will be forgotten)
        throw new RuntimeException("CacheManager expected to flush, but it didn't happen");
    }

    private int[] cleanup(boolean aborted, boolean nativeThrowed, StorageWriterContext storageWriterContext)
    {
        int[] outFileParams = null;
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
                    outFileParams = weClose(storageWriterContext);
                    if (outFileParams[WeProperties.WE_PROPERTIES_END_OFFSET.ordinal()] == -1) {
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
        return outFileParams;
    }

    void updateToFailedState(WarmUpElement.Builder warmupElementBuilder, WarmupElementWriteMetadata warmupElementWriteMetadata)
    {
        warmupElementBuilder.state(new WarmUpElementState(WarmUpElementState.State.FAILED_TEMPORARILY,
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
            storageWriterContext.getLuceneIndexer().get().closeLuceneIndex(storageWriterContext.getFileCookieParams());
        }

        if (storageWriterContext.weSuccess()) {
            writeJuffersWarmUpElement.commitWE(storageWriterContext.getRecordBufferPos());
        }
        storageWriterContext.resetRecords();
    }

    private int[] weClose(StorageWriterContext storageWriterContext)
    {
        int[] outFileParams = new int[WeProperties.values().length];
        if (!storageWriterContext.isWeClosed()) {
            storageWriterContext.getBlockAppender().writeChunkMapValuesIntoChunkMapJuffer(storageWriterContext.getWriteJuffersWarmUpElement().getChunkMapList());
            long[] fileCookieParams = storageWriterContext.getFileCookieParams();
            // if current chunk is still opened it means there was an exception and we warm up element is aborted
            WriteJuffersWarmUpElement writeJuffersWarmUpElement = storageWriterContext.getWriteJuffersWarmUpElement();
            boolean currentChunkIsOpened = writeJuffersWarmUpElement.closeCurrentChunk();
            int numChunks = writeJuffersWarmUpElement.getNumChunks();
            if (currentChunkIsOpened || (numChunks == 0)) {
                outFileParams[WeProperties.WE_PROPERTIES_END_OFFSET.ordinal()] = -1;
            }
            else {
                outFileParams[WeProperties.WE_PROPERTIES_END_OFFSET.ordinal()] = (int) storageEngine.warmupElementClose(
                        storageWriterContext.getRecTypeCode(),
                        storageWriterContext.getRecTypeLength(),
                        storageWriterContext.getWarmUpType(),
                        numChunks,
                        fileCookieParams,
                        storageWriterContext.getBuffAddresses(),
                        outFileParams);
            }
            fileCookieParams[FILE_COOKIE_PARAMS_START_OFFSET.ordinal()] = outFileParams[WeProperties.WE_PROPERTIES_END_OFFSET.ordinal()];
            storageWriterContext.setWeClosed();
        }
        return outFileParams;
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
                    storageWriterContext.getWarmupElementStatsBuilder(),
                    storageWriterContext.getWriteJuffersWarmUpElement().getCurrentChunkHeader());
            storageWriterContext.getWriteJuffersWarmUpElement().increaseNullsCount(appendResult.nullsCount());
        }
        catch (WarmupException e) {
            storageWriterContext.setFailed();
            logger.debug("failed appending block to WE %s exception %s", warmupElementWriteMetadata, e);

            WarmUpElement.Builder warmupElementBuilder = WarmUpElement.builder(warmUpElement);
            if (e instanceof DictionaryMaxException dictionaryMaxException) {
                DictionaryInfo dictionaryInfo = new DictionaryInfo(dictionaryMaxException.getDictionaryKey(),
                        DictionaryState.DICTIONARY_MAX_EXCEPTION,
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
}
