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
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
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
import io.trino.plugin.warp.gen.constants.RecTypeCode;
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
import static java.util.Objects.requireNonNull;

@Singleton
public class StorageWriterService
{
    private static final Logger logger = Logger.get(StorageWriterService.class);
    private static final String LUCENE_STATS_GROUP_NAME = "lucene-index";
    private static final int STAT_MAX_SLICE_LENGTH = 8;

    private final StorageEngine storageEngine;
    private final StorageEngineConstants storageEngineConstants;
    private final BufferAllocator bufferAllocator;
    private final DictionaryCacheService dictionaryCacheService;
    private final BlockAppenderFactory blockAppenderFactory;
    private final PrintMetricsTimerTask metricsTimerTask;
    private final LuceneIndexerStats statsLuceneIndexer;

    enum WeProperties
    {
        WE_PROPERTIES_QUERY_OFFSET,
        WE_PROPERTIES_QUERY_READ_SIZE,
        WE_PROPERTIES_WARM_EVENTS,
        WE_PROPERTIES_WARM_ID,
        WE_PROPERTIES_END_OFFSET // MUST BE LAST
    }

    @Inject
    public StorageWriterService(StorageEngine storageEngine,
            StorageEngineConstants storageEngineConstants,
            BufferAllocator bufferAllocator,
            DictionaryCacheService dictionaryCacheService,
            MetricsManager metricsManager,
            PrintMetricsTimerTask metricsTimerTask,
            BlockAppenderFactory blockAppenderFactory)
    {
        this.storageEngine = requireNonNull(storageEngine);
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.bufferAllocator = requireNonNull(bufferAllocator);
        this.dictionaryCacheService = requireNonNull(dictionaryCacheService);
        this.blockAppenderFactory = requireNonNull(blockAppenderFactory);
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

    StorageWriterContext open(long[] fileCookie,
            int fileOffset,
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
        warmupElementBuilder.startOffset(fileOffset);
        // open storage engine WE

        boolean hasDictionary = dictionaryState == DictionaryState.DICTIONARY_VALID;
        StorageOpenResult storageOpenResult = storageWeOpen(warmUpElement,
                hasDictionary,
                storageWriterSplitConfig.contextAllocator().allocate(warmUpElement.getWarmUpContextSize(), Integer.BYTES).address(),
                fileCookie,
                fileOffset,
                storageWriterSplitConfig.writeBuff().address(),
                allocParams);

        // set up buffers
        WriteJuffersWarmUpElement writeJuffersWarmUpElement = getWriteJuffersWarmUpElement(storageOpenResult, hasDictionary, allocParams);
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
            LuceneIndexer luceneIndexer = new LuceneIndexer(storageEngine,
                    storageEngineConstants,
                    writeJuffersWarmUpElement,
                    statsLuceneIndexer);
            luceneIndexer.resetLuceneIndex();
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
                blockAppender,
                true,
                writeDictionaryOpt,
                luceneIndexerOpt);
    }

    WriteJuffersWarmUpElement getWriteJuffersWarmUpElement(StorageOpenResult storageOpenResult, boolean dictionaryValid, WarmUpElementAllocationParams allocParams)
    {
        WriteJuffersWarmUpElement juffersWE = new WriteJuffersWarmUpElement(storageEngine, storageEngineConstants, bufferAllocator, storageOpenResult.buffs(), storageOpenResult.weCookie(), allocParams);
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
        DictionaryState dictionaryState = dictionaryCacheService.calculateDictionaryStateForWrite(warmUpElement, dictionaryEnabled);
        return Pair.of(dictionaryKey, dictionaryState);
    }

    private StorageOpenResult storageWeOpen(WarmUpElement warmUpElement,
            boolean hasDictionary,
            long context,
            long[] fileCookie,
            int fileOffset,
            long writeBuffAddress,
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
                fileCookie,
                fileOffset,
                recTypeCode,
                recTypeLength,
                warmUpType,
                writeBuffAddress,
                buffAddresses);
        return new StorageOpenResult(buffs, weCookie);
    }

    WarmSinkResult close(int totalRecords, StorageWriterSplitConfig storageWriterSplitConfig, StorageWriterContext storageWriterContext)
    {
        int[] outFileParams = cleanup(false, false, storageWriterContext);

        WarmUpElement.Builder warmupElementBuilder = storageWriterContext.getWarmupElementBuilder();
        if (!storageWriterContext.weSuccess() || outFileParams == null) {
            return new WarmSinkResult(warmupElementBuilder.build(), 0);
        }

        WarmupElementStats closedStats;
        WarmupElementWriteMetadata warmupElementWriteMetadata = storageWriterContext.getWarmupElementWriteMetadata();
        try {
            closedStats = getFinalStats(storageWriterContext.getWarmupElementStatsBuilder().build(), warmupElementWriteMetadata);
        }
        catch (Exception e) {
            logger.warn(e, "failed to get range on write");
            throw new IllegalArgumentException();
        }

        warmupElementBuilder.state(WarmUpElementState.VALID)
                .warmState(WarmState.HOT)
                .queryOffset(outFileParams[WeProperties.WE_PROPERTIES_QUERY_OFFSET.ordinal()])
                .queryReadSize(outFileParams[WeProperties.WE_PROPERTIES_QUERY_READ_SIZE.ordinal()])
                .warmEvents(outFileParams[WeProperties.WE_PROPERTIES_WARM_EVENTS.ordinal()])
                .warmId(outFileParams[WeProperties.WE_PROPERTIES_WARM_ID.ordinal()])
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
            updateToFailedState(warmupElementBuilder, storageWriterContext.getWarmupElementWriteMetadata());
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
                updateToFailedState(warmupElementBuilder, storageWriterContext.getWarmupElementWriteMetadata());
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
        if (storageWriterContext.weSuccess()) {
            warmupElementBuilder.endOffset(offset);
        }
        if (offset == 0 && storageWriterContext.weSuccess()) {
            updateToFailedState(storageWriterContext.getWarmupElementBuilder(), storageWriterContext.getWarmupElementWriteMetadata());
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
        for (; blockIndex < warmupElementBlocks.getBlocks().size() && !flushed && storageWriterContext.weSuccess(); blockIndex++) {
            Block block = warmupElementBlocks.getBlocks().get(blockIndex);
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

        if (storageWriterContext.weSuccess() && !flushed) {
            flushAfterAppendingBlocks(warmupElementBlocks, storageWriterContext);
        }

        if (currentRecordNumber == warmupElementBlocks.getBlocks().get(blockIndex - 1).getPositionCount()) {
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
                !warmupElementBlocks.isReady()) { // When isReady() == false, we know that we should flush, so we already do it here without counting on cleanup() to do the job
            flushRecordBuffer(storageWriterContext);
            return;
        }

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
                    if (aborted) {
                        storageWriterContext.setFailed();
                        updateToFailedState(storageWriterContext.getWarmupElementBuilder(), storageWriterContext.getWarmupElementWriteMetadata());
                    }
                }
                else if (aborted) {
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
        if (storageWriterContext.getLuceneIndexer().isPresent()) {
            storageWriterContext.getLuceneIndexer().get().closeLuceneIndex(storageWriterContext.getWeCookie());
        }

        if (storageWriterContext.weSuccess()) {
            storageWriterContext.getWriteJuffersWarmUpElement().commitWE(storageWriterContext.getRecordBufferPos());
        }
        storageWriterContext.resetRecords();
    }

    private int[] weClose(StorageWriterContext storageWriterContext)
    {
        int[] outFileParams = new int[WeProperties.values().length];
        if (!storageWriterContext.isWeClosed()) {
            storageWriterContext.getBlockAppender().writeChunkMapValuesIntoChunkMapJuffer(storageWriterContext.getWriteJuffersWarmUpElement().getChunkMapList());
            outFileParams[WeProperties.WE_PROPERTIES_END_OFFSET.ordinal()] = (int) storageEngine.warmupElementClose(storageWriterContext.getWeCookie(), outFileParams);
            storageWriterContext.setWeClosed();
        }
        return outFileParams;
    }

    private WarmupElementStats getFinalStats(WarmupElementStats warmupElementStats, WarmupElementWriteMetadata warmupElementWriteMetadata)
    {
        WarmUpElement warmUpElement = warmupElementWriteMetadata.warmUpElement();
        if (warmupElementStats.isInitialized() &&
                warmUpElement.getRecTypeCode().isSupportedFiltering() &&
                warmUpElement.getWarmUpType() != WarmUpType.WARM_UP_TYPE_LUCENE) {
            if (warmUpElement.getRecTypeCode() == RecTypeCode.REC_TYPE_VARCHAR ||
                    warmUpElement.getRecTypeCode() == RecTypeCode.REC_TYPE_CHAR) {
                Slice maxSlice = (Slice) warmupElementStats.getMaxValue();
                String maxValue;
                String minValue;
                Slice minSlice = (Slice) warmupElementStats.getMinValue();
                //if type is Slice we want to save the first 8 bytes for min/max values, for max value we add 1 to last position
                //need to convert them to byte array in order to preserve the original values
                byte[] maxSliceValue;
                if (maxSlice.length() > STAT_MAX_SLICE_LENGTH) {
                    maxSliceValue = maxSlice.getBytes(0, STAT_MAX_SLICE_LENGTH);
                    if (maxSliceValue[STAT_MAX_SLICE_LENGTH - 1] == Byte.MAX_VALUE) {
                        //protect from overflow
                        maxValue = null;
                    }
                    else {
                        //need to increase value by 1 in order to make sure ranges will overlaps (see @RangeMatcher.java)
                        maxSliceValue[STAT_MAX_SLICE_LENGTH - 1]++;
                        maxValue = Slices.wrappedBuffer(maxSliceValue).toStringUtf8();
                    }
                }
                else {
                    maxValue = maxSlice.toStringUtf8();
                }

                if (minSlice.length() > STAT_MAX_SLICE_LENGTH) {
                    byte[] minSliceValue = minSlice.getBytes(0, STAT_MAX_SLICE_LENGTH);
                    minValue = Slices.wrappedBuffer(minSliceValue).toStringUtf8();
                }
                else {
                    minValue = minSlice.toStringUtf8();
                }
                warmupElementStats = new WarmupElementStats(warmupElementStats.getNullsCount(), minValue, maxValue);
            }
        }
        return warmupElementStats;
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

        storageEngine.commitRecordBufferPrepare(storageWriterContext.getWeCookie());

        storageWriterContext.resetRecordBufferPos();
        storageWriterContext.setRecordBufferSize(1 << storageEngineConstants.getChunkSizeShift());

        storageWriterContext.getWriteJuffersWarmUpElement().resetAllBuffers();
        if (storageWriterContext.getLuceneIndexer().isPresent()) {
            resetLucene(storageWriterContext.getLuceneIndexer().get());
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

    void resetLucene(LuceneIndexer luceneIndexer)
    {
        luceneIndexer.resetLuceneIndex();
        luceneIndexer.resetLuceneBufferPosition();
    }
}
