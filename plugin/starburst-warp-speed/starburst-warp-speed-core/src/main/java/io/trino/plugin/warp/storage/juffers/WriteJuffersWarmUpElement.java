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
package io.trino.plugin.warp.storage.juffers;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.Slice;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.juffer.WarmUpElementAllocationParams;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.memory.ThreadArena;
import io.trino.plugin.warp.storage.write.CompressionState;
import io.trino.plugin.warp.storage.write.WarmUpState;
import io.trino.plugin.warp.type.TypeUtils;
import io.trino.plugin.warp.util.SliceUtils;
import io.trino.spi.type.Int128;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.lang.Double.doubleToLongBits;
import static java.lang.Double.longBitsToDouble;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.intBitsToFloat;

public class WriteJuffersWarmUpElement
        extends JuffersWarmUpElementBase
{
    private static final byte INVALID_MAX = 0;
    private static final byte INVALID_MIN = 1;

    private final StorageEngine storageEngine;
    private final int pageOffsetMask;
    private final int pageSize;
    private final int pageSizeShift;
    private final int chunksBufferMaxPages;
    private final WarmUpState warmUpState;
    private final RecordBufferParams recordBufferParams;
    private final CompressionState compressionState;
    private final ChunkHeader chunkHeader;
    private final List<MemorySegment> chunksList;
    private final WarmUpElementAllocationParams allocParams;
    private final byte warmId;

    // min/max and single value per chunk
    private boolean chunkOpened;
    private int numChunks;
    private long recordBufferMin;
    private long recordBufferMax;
    private int recordBufferSingleOffset;
    private long recordBufferSingleCrc;
    private Int128 recordBufferSingleLongDec;
    private int actualRecTypeLength; // largest record encountered

    public WriteJuffersWarmUpElement(
            StorageEngine storageEngine,
            StorageEngineConstants storageEngineConstants,
            BufferAllocator bufferAllocator,
            RecordBufferParams recordBufferParams,
            WarmUpState warmUpState,
            WarmUpElementAllocationParams allocParams,
            CompressionState compressionState,
            byte warmId,
            ThreadArena arena)
    {
        super();

        this.storageEngine = storageEngine;
        this.pageOffsetMask = storageEngineConstants.getPageOffsetMask();
        this.pageSize = storageEngineConstants.getPageSize();
        this.pageSizeShift = storageEngineConstants.getPageSizeShift();
        this.chunksBufferMaxPages = storageEngineConstants.getChunksBufferMaxSize() >> pageSizeShift;
        this.chunksList = new ArrayList<>();
        this.allocParams = allocParams;
        this.warmUpState = warmUpState;
        this.compressionState = compressionState;
        this.recordBufferParams = recordBufferParams;
        this.warmId = warmId;

        compressionState.reset();

        // we always have an invalid cookie at the end of the list for a case we aborted the last chunk in the middle
        // in that case native might read this cookie and we prefer to have it initialized with invalid values
        this.chunkHeader = new ChunkHeader(arena, allocParams.isCrcBufferNeeded());
        warmUpState.setChunkHeader(chunkHeader.getAddress());
        newChunk();

        if (allocParams.isRecBufferNeeded()) {
            RecordWriteJuffer recordJuffers = new RecordWriteJuffer(
                    bufferAllocator,
                    allocParams,
                    storageEngine,
                    warmUpState.getMemory(),
                    compressionState.getMemory());
            juffers.put(recordJuffers.getJufferType(), recordJuffers);

            if (allocParams.isExtBufferNeeded()) {
                ExtendedJuffer extendedJuffers = new ExtendedJuffer(
                        bufferAllocator,
                        allocParams,
                        storageEngine,
                        warmUpState.getMemory(),
                        recordBufferParams);
                juffers.put(extendedJuffers.getJufferType(), extendedJuffers);
            }
        }

        NullWriteJuffer nullJuffers = new NullWriteJuffer(bufferAllocator);
        juffers.put(nullJuffers.getJufferType(), nullJuffers);
        ChunksJuffer chunksJuffer = new ChunksJuffer(bufferAllocator);
        juffers.put(chunksJuffer.getJufferType(), chunksJuffer);

        if (allocParams.isLuceneIndexNeeded()) {
            LuceneWriteJuffer luceneJuffers = new LuceneWriteJuffer(bufferAllocator);
            juffers.put(luceneJuffers.getJufferType(), luceneJuffers);
        }

        if (allocParams.isCrcBufferNeeded()) {
            CrcJuffer crcJuffers = new CrcJuffer(bufferAllocator);
            juffers.put(crcJuffers.getJufferType(), crcJuffers);
        }

        if (allocParams.isMdBufferNeeded()) {
            VarlenMdJuffer varlenMdJuffers = new VarlenMdJuffer(bufferAllocator);
            juffers.put(varlenMdJuffers.getJufferType(), varlenMdJuffers);
        }

        recordBufferMin = INVALID_MIN;
        recordBufferMax = INVALID_MAX;
        recordBufferSingleOffset = -1;
    }

    public void createBuffers(boolean isDictionaryValid)
    {
        for (BaseJuffer juffer : juffers.values()) {
            BaseWriteJuffer writeJuffer = (BaseWriteJuffer) juffer;
            writeJuffer.createBuffer(warmUpState.getJbufs(), isDictionaryValid);
        }

        if (allocParams.isRecBufferNeeded() && TypeUtils.isVarlenStr(allocParams.recTypeCode())) {
            // in case of varchar data. we initialize for all nulls case. when the first record comes it will put a larger value
            this.actualRecTypeLength = 0;
        }
        else {
            // in case of fixed size we can already initialize to the fixed size value
            // in case of all nulls or index it also applies
            this.actualRecTypeLength = allocParams.recTypeLength();
        }
    }

    public void resetAllBuffers()
    {
        for (BaseJuffer juffer : juffers.values()) {
            BaseWriteJuffer writeJuffer = (BaseWriteJuffer) juffer;
            writeJuffer.reset();
        }

        // the initial value must comply with the following requirements:
        // 1. min > max to make it an empty range
        // 2. both min and max are smaller than 256 (1 byte value), since native works in little endian and looks at this value
        recordBufferMin = INVALID_MIN;
        recordBufferMax = INVALID_MAX;
        recordBufferSingleOffset = -1;
        recordBufferSingleCrc = 0;
        recordBufferSingleLongDec = null;
    }

    public void commitWE(int recordBufferPos)
    {
        int numBytesWritten = 0; // if record buffer exist, we will get a positive value in the if below if varlen md exists

        chunkOpened = true; // in case we will fail in the middle of this call
        if (allocParams.isRecBufferNeeded()) {
            RecordWriteJuffer recordJuffer = getRecordJuffer();
            boolean prepareMdBuffer = allocParams.isMdBufferNeeded() && !recordJuffer.isDictionaryValid();
            numBytesWritten = calcNumBytesWritten(recordJuffer.getRecordBufferEntrySize(), recordJuffer.getWrappedBuffer(), prepareMdBuffer ? getVarlenMdBuffer() : null);
            if (allocParams.isExtBufferNeeded()) {
                getExtRecordJuffer().commitAndResetExtRecordBuffer();
            }
        }

        recordBufferParams.setParams(
                recordBufferMin,
                recordBufferMax,
                recordBufferPos,
                getNullJuffer().getNullsCount(),
                numBytesWritten,
                recordBufferSingleOffset);

        storageEngine.warmupChunk(warmUpState.getMemory(), recordBufferParams.getMemory(), compressionState.getMemory());

        chunksList.add(chunkHeader.verifyAndCopyChunkHeader());
        closeCurrentChunk();
        newChunk();
    }

    private void newChunk()
    {
        chunkHeader.resetHeader(warmId);
        compressionState.setAlgorithm(numChunks);
    }

    private int calcNumBytesWritten(int bufferEntrySize, Buffer recordJuffer, IntBuffer mdBuffer)
    {
        int res = recordJuffer.position() * bufferEntrySize;

        if ((mdBuffer != null) && ((res & pageOffsetMask) != 0)) {
            // add one more byte for the page end mark
            int lastOffset = mdBuffer.get(mdBuffer.position());
            mdBuffer.put(mdBuffer.position(), lastOffset + 1);
            res++;
        }

        return res;
    }

    public void commitAndResetWE(int numRecs, int addedNV, int numBytes, int numExtBytes)
    {
        chunkOpened = true;
        if (allocParams.isExtBufferNeeded()) {
            getExtRecordJuffer().commitAndResetExtRecordBuffer(numExtBytes);
        }

        recordBufferParams.setParams(
                recordBufferMin,
                recordBufferMax,
                numRecs,
                getNullJuffer().getNullsCount() + addedNV,
                numBytes,
                recordBufferSingleOffset);

        getRecordJuffer().commitAndResetWE(recordBufferParams.getMemory());
    }

    public void writeChunkListToJuffer(int baseOffset)
    {
        ByteBuffer chunksBuffer = getChunksBuffer();
        chunksBuffer.position(0); // to be on the safe side

        final long chunkHeaderSize = chunkHeader.byteSize();
        MemorySegment chunksSegment = MemorySegment.ofBuffer(chunksBuffer).reinterpret(chunkHeaderSize * chunksList.size());
        Iterator<MemorySegment> copyChunkHeaderItr = chunksList.iterator();
        chunksSegment.elements(MemoryLayout.paddingLayout(chunkHeaderSize))
                .forEach(chunkHeader -> ChunkHeader.finalizeChunkHeader(copyChunkHeaderItr.next(), chunkHeader, baseOffset));
    }

    public void increaseNullsCount(int nullsCount)
    {
        getNullJuffer().increaseNullsCount(nullsCount);
    }

    public void advancedExtRecordLastPos(int newPosition)
    {
        getExtRecordJuffer().advancedExtRecordLastPos(newPosition);
    }

    // lucene APIs
    public void updateLuceneProps(Slice val)
    {
        getLuceneJuffer().updateLuceneProps(val);
    }

    // updates the current opened/closed state of current chunk to closed and returns the previous state
    public boolean closeCurrentChunk()
    {
        boolean res = chunkOpened;
        if (chunkOpened) {
            numChunks++; // counting the chunk we just closed
        }
        chunkOpened = false;
        return res;
    }

    public int getNumChunks()
    {
        return numChunks;
    }

    public int getAndVerifyNumChunkPages()
    {
        int numPages = (numChunks + pageSize - 1) >> pageSizeShift;
        if ((numPages <= 0) || (numPages >= chunksBufferMaxPages)) {
            throw new RuntimeException("number of chunk pages " + numPages + " is not positive or exceeds limit " + chunksBufferMaxPages);
        }
        return numPages;
    }

    // min/max and single value
    public void updateRecordBufferProps(long val)
    {
        updateRecordBufferProps(val, 0);
    }

    public void updateRecordBufferProps(long val, int length)
    {
        // update main agg min/max values (prefix is the same)
        if (recordBufferMin > recordBufferMax) {
            recordBufferMin = val;
            recordBufferMax = val;
        }
        else {
            if (val < recordBufferMin) {
                recordBufferMin = val;
            }
            else if (val > recordBufferMax) {
                recordBufferMax = val;
            }
        }
        // in case of extended varchar we might reach this API not on the first record of the chunk, but still it means we need to
        // disable single chunk optimization always
        resetSingleValue();
        actualRecTypeLength = Math.max(length, actualRecTypeLength);
    }

    // NaN is treated as minus infinity - the smallest number
    public void updateRecordBufferProps(double val)
    {
        long longVal = doubleToLongBits(val);
        double min = longBitsToDouble(recordBufferMin);
        double max = longBitsToDouble(recordBufferMax);

        // update main agg min/max values (prefix is the same)
        if (min > max) {
            recordBufferMin = longVal;
            recordBufferMax = longVal;
        }
        else {
            if ((val < min) || Double.isNaN(val)) {
                recordBufferMin = longVal;
            }
            else if ((val > max) || Double.isNaN(max)) {
                recordBufferMax = longVal;
            }
        }
    }

    // NaN is treated as minus infinity - the smallest number
    public void updateRecordBufferProps(float val)
    {
        int intVal = floatToIntBits(val);
        float min = intBitsToFloat((int) recordBufferMin);
        float max = intBitsToFloat((int) recordBufferMax);

        // update main agg min/max values (prefix is the same)
        if (min > max) {
            recordBufferMin = intVal;
            recordBufferMax = intVal;
        }
        else {
            if ((val < min) || Float.isNaN(val)) {
                recordBufferMin = intVal;
            }
            else if ((val > max) || Float.isNaN(max)) {
                recordBufferMax = intVal;
            }
        }
    }

    public void updateRecordBufferProps(long val, Int128 fullValue, int offset)
    {
        if (recordBufferMin > recordBufferMax) {
            // first value of the chunk, take it as min and max an calculate its crc as single value optimization
            recordBufferMin = val;
            recordBufferMax = val;
            recordBufferSingleLongDec = fullValue;
            recordBufferSingleOffset = offset;
        }
        else {
            // update main agg min/max values (prefix is the same)
            if (val < recordBufferMin) {
                recordBufferMin = val;
                resetSingleValue();
            }
            else if (val > recordBufferMax) {
                recordBufferMax = val;
                resetSingleValue();
                // we first check if the current value is not zero to skip the crc calculation if possible
                // we loose the single value optimization in case the crc is exactly 0 (should be almost impossible statistically)
            }
            else if (recordBufferSingleLongDec != null && !fullValue.equals(recordBufferSingleLongDec)) {
                resetSingleValue();
            }
        }
    }

    public void updateRecordBufferProps(long val, ByteBuffer byteBuffer, int length, int offset)
    {
        if (recordBufferMin > recordBufferMax) {
            // first value of the chunk, take it as min and max an calculate its crc as single value optimization
            recordBufferMin = val;
            recordBufferMax = val;
            recordBufferSingleCrc = SliceUtils.calcCrc(byteBuffer, length);
            recordBufferSingleOffset = offset;
        }
        else {
            // update main agg min/max values (prefix is the same)
            if (val < recordBufferMin) {
                recordBufferMin = val;
                resetSingleValue();
            }
            else if (val > recordBufferMax) {
                recordBufferMax = val;
                resetSingleValue();
                // we first check if the current value is not zero to skip the crc calculation if possible
                // we loose the single value optimization in case the crc is exactly 0 (should be almost impossible statistically)
            }
            else if ((recordBufferSingleCrc != 0) && (recordBufferSingleCrc != SliceUtils.calcCrc(byteBuffer, length))) {
                resetSingleValue();
            }
        }
        actualRecTypeLength = Math.max(length, actualRecTypeLength);
    }

    public void updateRecordBufferProps(long val, long crc, int length, int offset)
    {
        if (recordBufferMin > recordBufferMax) {
            // first value of the chunk, take it as min and max an calculate its crc as single value optimization
            recordBufferMin = val;
            recordBufferMax = val;
            recordBufferSingleCrc = crc;
            recordBufferSingleOffset = offset;
        }
        else {
            // update main agg min/max values (prefix is the same)
            if (val < recordBufferMin) {
                recordBufferMin = val;
                resetSingleValue();
            }
            else if (val > recordBufferMax) {
                recordBufferMax = val;
                resetSingleValue();
                // we loose the single value optimization in case the crc is exactly 0 (should be almost impossible statistically)
            }
            else if ((recordBufferSingleCrc != 0) && (recordBufferSingleCrc != crc)) {
                resetSingleValue();
            }
        }
        actualRecTypeLength = Math.max(length, actualRecTypeLength);
    }

    private void resetSingleValue()
    {
        recordBufferSingleOffset = -1;
        recordBufferSingleCrc = 0;
        recordBufferSingleLongDec = null;
    }

    public void resetSingleValueIfNeeded(ByteBuffer byteBuffer, int length)
    {
        if ((recordBufferSingleCrc != 0) && (recordBufferSingleCrc != SliceUtils.calcCrc(byteBuffer, length))) {
            resetSingleValue();
        }
    }

    // getters
    public int getActualRecTypeLength()
    {
        return actualRecTypeLength;
    }

    public int getRecBuffSize()
    {
        return allocParams.recBuffSize();
    }

    public RecordWriteJuffer getRecordJuffer()
    {
        return (RecordWriteJuffer) getJufferByType(JuffersType.RECORD);
    }

    public LuceneWriteJuffer getLuceneJuffer()
    {
        return (LuceneWriteJuffer) getJufferByType(JuffersType.LUCENE);
    }

    public ExtendedJuffer getExtRecordJuffer()
    {
        return (ExtendedJuffer) getJufferByType(JuffersType.EXTENDED_REC);
    }

    public NullWriteJuffer getNullJuffer()
    {
        return (NullWriteJuffer) getJufferByType(JuffersType.NULL);
    }

    public VarlenMdJuffer getVarlenMdJuffer()
    {
        return (VarlenMdJuffer) getJufferByType(JuffersType.VARLENMD);
    }

    public CrcJuffer getCrcJuffer()
    {
        return (CrcJuffer) getJufferByType(JuffersType.CRC);
    }

    public Buffer getLuceneBuffer()
    {
        return getBufferByType(JuffersType.LUCENE);
    }

    public ByteBuffer getExtRecordBuffer()
    {
        return (ByteBuffer) getBufferByType(JuffersType.EXTENDED_REC);
    }

    public IntBuffer getVarlenMdBuffer()
    {
        return (IntBuffer) getBufferByType(JuffersType.VARLENMD);
    }

    public ByteBuffer getCrcBuffer()
    {
        return (ByteBuffer) getBufferByType(JuffersType.CRC);
    }

    private ByteBuffer getChunksBuffer()
    {
        return (ByteBuffer) getBufferByType(JuffersType.CHUNKS);
    }

    // used for tests to avoid hitting invalid chunk type exceptipon
    @VisibleForTesting
    public void setChunkTypeAsValid()
    {
        chunkHeader.setTypeAndWarmId((byte) ((warmId << 4) | 0x2));
    }
}
