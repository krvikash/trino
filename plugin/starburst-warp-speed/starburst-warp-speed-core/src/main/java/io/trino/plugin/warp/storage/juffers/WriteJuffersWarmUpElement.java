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

import io.airlift.slice.Slice;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.juffer.WarmUpElementAllocationParams;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.type.TypeUtils;
import io.trino.plugin.warp.util.SliceUtils;
import io.trino.spi.type.Int128;

import java.lang.foreign.Arena;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_START_OFFSET;
import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_WARM_EVENTS;
import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_WRITE_BUF_PAGE_IX;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Double.longBitsToDouble;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.intBitsToFloat;

public class WriteJuffersWarmUpElement
        extends JuffersWarmUpElementBase
{
    private static final byte INVALID_MAX = 0;
    private static final byte INVALID_MIN = 1;
    private static final StructLayout RECORD_BUFFER_PARAMS_LAYOUT;
    private static final long RECORD_BUFFER_PARAMS_OFFSET_MIN;
    private static final long RECORD_BUFFER_PARAMS_OFFSET_MAX;
    private static final long RECORD_BUFFER_PARAMS_OFFSET_NRECS;
    private static final long RECORD_BUFFER_PARAMS_OFFSET_NVS;
    private static final long RECORD_BUFFER_PARAMS_OFFSET_SIZE;
    private static final long RECORD_BUFFER_PARAMS_OFFSET_SINGLE_VAL_OFFSET;
    private static final long RECORD_BUFFER_PARAMS_OFFSET_OUT_WARM_EVENTS;

    private final StorageEngine storageEngine;
    private final int pageOffsetMask;
    private final long weCookie;
    private final MemorySegment warmUpElementAtt;
    private final MemorySegment recordBufferParams;
    private final long[] fileCookieParams;
    private final long[] buffAddresses;
    private final byte[] compressionStats;
    private final int chunkHeaderSize;
    private final List<ChunkMap> chunkMapList;
    private final MemorySegment[] buffs;
    private final WarmUpElementAllocationParams allocParams;

    // min/max and single value per chunk
    private byte[] chunkHeader;
    private boolean chunkOpened;
    private int numChunks;
    private long recordBufferMin;
    private long recordBufferMax;
    private int recordBufferSingleOffset;
    private long recordBufferSingleCrc;
    private Int128 recordBufferSingleLongDec;
    private int actualRecTypeLength; // largest record encountered

    static {
        RECORD_BUFFER_PARAMS_LAYOUT = MemoryLayout.structLayout(
                ValueLayout.JAVA_LONG.withName("min"),
                ValueLayout.JAVA_LONG.withName("max"),
                ValueLayout.JAVA_INT.withName("nrecs"),
                ValueLayout.JAVA_INT.withName("nvs"),
                ValueLayout.JAVA_INT.withName("size"),
                ValueLayout.JAVA_INT.withName("singleValOffset"),
                ValueLayout.JAVA_INT.withName("outWarmEvents")).withName("rec_buf_t");
        RECORD_BUFFER_PARAMS_OFFSET_MIN = RECORD_BUFFER_PARAMS_LAYOUT.byteOffset(PathElement.groupElement("min"));
        RECORD_BUFFER_PARAMS_OFFSET_MAX = RECORD_BUFFER_PARAMS_LAYOUT.byteOffset(PathElement.groupElement("max"));
        RECORD_BUFFER_PARAMS_OFFSET_NRECS = RECORD_BUFFER_PARAMS_LAYOUT.byteOffset(PathElement.groupElement("nrecs"));
        RECORD_BUFFER_PARAMS_OFFSET_NVS = RECORD_BUFFER_PARAMS_LAYOUT.byteOffset(PathElement.groupElement("nvs"));
        RECORD_BUFFER_PARAMS_OFFSET_SIZE = RECORD_BUFFER_PARAMS_LAYOUT.byteOffset(PathElement.groupElement("size"));
        RECORD_BUFFER_PARAMS_OFFSET_SINGLE_VAL_OFFSET = RECORD_BUFFER_PARAMS_LAYOUT.byteOffset(PathElement.groupElement("singleValOffset"));
        RECORD_BUFFER_PARAMS_OFFSET_OUT_WARM_EVENTS = RECORD_BUFFER_PARAMS_LAYOUT.byteOffset(PathElement.groupElement("outWarmEvents"));
    }

    public WriteJuffersWarmUpElement(StorageEngine storageEngine,
            StorageEngineConstants storageEngineConstants,
            BufferAllocator bufferAllocator,
            MemorySegment[] buffs,
            long weCookie,
            MemorySegment warmUpElementAtt,
            WarmUpElementAllocationParams allocParams,
            long[] fileCookieParams,
            long[] buffAddresses,
            byte[] compressionStats)
    {
        super();

        this.storageEngine = storageEngine;
        this.pageOffsetMask = storageEngineConstants.getPageOffsetMask();
        this.chunkMapList = new ArrayList<>();
        this.buffs = buffs;
        this.allocParams = allocParams;
        this.weCookie = weCookie;
        this.warmUpElementAtt = warmUpElementAtt;
        this.fileCookieParams = fileCookieParams;
        this.buffAddresses = buffAddresses;
        this.compressionStats = compressionStats;
        this.chunkHeaderSize = storageEngineConstants.getChunkHeaderMaxSize();

        // we always have an invalid cookie at the end of the list for a case we aborted the last chunk in the middle
        // in that case native might read this cookie and we prefer to have it initialized with invalid values
        byte[] defaultChunkCookies = new byte[chunkHeaderSize];
        Arrays.fill(defaultChunkCookies, (byte) -1);
        this.chunkMapList.add(new ChunkMap(defaultChunkCookies));
        this.chunkHeader = new byte[chunkHeaderSize];

        this.recordBufferParams = Arena.ofAuto().allocate(RECORD_BUFFER_PARAMS_LAYOUT.byteSize(), ValueLayout.JAVA_INT.byteSize());

        if (allocParams.isRecBufferNeeded()) {
            RecordWriteJuffer recordJuffers = new RecordWriteJuffer(bufferAllocator,
                    allocParams,
                    storageEngine,
                    weCookie,
                    warmUpElementAtt,
                    fileCookieParams,
                    buffAddresses,
                    compressionStats);
            juffers.put(recordJuffers.getJufferType(), recordJuffers);

            if (allocParams.isExtBufferNeeded()) {
                ExtendedJuffer extendedJuffers = new ExtendedJuffer(bufferAllocator,
                        allocParams,
                        storageEngine,
                        weCookie,
                        warmUpElementAtt,
                        fileCookieParams,
                        buffAddresses);
                juffers.put(extendedJuffers.getJufferType(), extendedJuffers);
            }
        }

        NullWriteJuffer nullJuffers = new NullWriteJuffer(bufferAllocator);
        juffers.put(nullJuffers.getJufferType(), nullJuffers);
        ChunksMapJuffer chunksMapJuffers = new ChunksMapJuffer(bufferAllocator);
        juffers.put(chunksMapJuffers.getJufferType(), chunksMapJuffers);

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
            writeJuffer.createBuffer(buffs, isDictionaryValid);
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
                getExtRecordJuffer().commitAndResetExtRecordBuffer(chunkHeader);
            }
        }

        setMin(recordBufferMin);
        setMax(recordBufferMax);
        setNumRecords(recordBufferPos);
        setNumNulls(getNullJuffer().getNullsCount());
        setNumBytes(numBytesWritten);
        setSingleValOffset(recordBufferSingleOffset);
        long res = storageEngine.warmupChunk(weCookie,
                recordBufferParams.address(),
                warmUpElementAtt.address(),
                fileCookieParams,
                buffAddresses,
                true,
                compressionStats,
                chunkHeader);
        fileCookieParams[FILE_COOKIE_PARAMS_START_OFFSET.ordinal()] = res & 0xFFFFFFFFL;
        fileCookieParams[FILE_COOKIE_PARAMS_WRITE_BUF_PAGE_IX.ordinal()] = res >> 32;
        fileCookieParams[FILE_COOKIE_PARAMS_WARM_EVENTS.ordinal()] |= getWarmEvents();
        chunkMapList.add(chunkMapList.size() - 1, new ChunkMap(chunkHeader));
        chunkHeader = new byte[chunkHeaderSize];
        closeCurrentChunk();
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
            getExtRecordJuffer().commitAndResetExtRecordBuffer(chunkHeader, numExtBytes);
        }

        setMin(recordBufferMin);
        setMax(recordBufferMax);
        setNumRecords(numRecs);
        setNumNulls(getNullJuffer().getNullsCount() + addedNV);
        setNumBytes(numBytes);
        setSingleValOffset(recordBufferSingleOffset);
        getRecordJuffer().commitAndResetWE(chunkHeader, recordBufferParams);
        fileCookieParams[FILE_COOKIE_PARAMS_WARM_EVENTS.ordinal()] |= getWarmEvents();
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

    public byte[] getCurrentChunkHeader()
    {
        chunkOpened = true;
        return chunkHeader;
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

    public List<ChunkMap> getChunkMapList()
    {
        return chunkMapList;
    }

    public ByteBuffer getChunkMapBuffer()
    {
        return (ByteBuffer) getBufferByType(JuffersType.CHUNKS_MAP);
    }

    public int getChunkHeaderSize()
    {
        return chunkHeaderSize;
    }

    private void setMin(long min)
    {
        recordBufferParams.set(ValueLayout.JAVA_LONG, RECORD_BUFFER_PARAMS_OFFSET_MIN, min);
    }

    private void setMax(long max)
    {
        recordBufferParams.set(ValueLayout.JAVA_LONG, RECORD_BUFFER_PARAMS_OFFSET_MAX, max);
    }

    private void setNumRecords(int numRecords)
    {
        if (numRecords <= 0) {
            throw new RuntimeException("warmup chunk called with illegal number of rows " + numRecords);
        }
        recordBufferParams.set(ValueLayout.JAVA_INT, RECORD_BUFFER_PARAMS_OFFSET_NRECS, numRecords);
    }

    private void setNumNulls(int numNulls)
    {
        recordBufferParams.set(ValueLayout.JAVA_INT, RECORD_BUFFER_PARAMS_OFFSET_NVS, numNulls);
    }

    private void setNumBytes(int numBytes)
    {
        recordBufferParams.set(ValueLayout.JAVA_INT, RECORD_BUFFER_PARAMS_OFFSET_SIZE, numBytes);
    }

    private void setSingleValOffset(int singleValOffset)
    {
        recordBufferParams.set(ValueLayout.JAVA_INT, RECORD_BUFFER_PARAMS_OFFSET_SINGLE_VAL_OFFSET, singleValOffset);
    }

    private int getWarmEvents()
    {
        return (int) recordBufferParams.get(ValueLayout.JAVA_INT, RECORD_BUFFER_PARAMS_OFFSET_OUT_WARM_EVENTS);
    }
}
