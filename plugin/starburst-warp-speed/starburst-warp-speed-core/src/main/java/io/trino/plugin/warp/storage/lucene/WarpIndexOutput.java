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
package io.trino.plugin.warp.storage.lucene;

import io.airlift.log.Logger;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.storage.juffers.WriteJuffersWarmUpElement;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_START_OFFSET;
import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_WRITE_BUF_PAGE_IX;

public class WarpIndexOutput
        extends IndexOutput
{
    private static final Logger logger = Logger.get(WarpIndexOutput.class);
    private final StorageEngine storageEngine;
    private final LuceneFileType luceneFileType;
    private final StorageEngineConstants storageEngineConstants;
    private final WriteJuffersWarmUpElement juffersWE;
    private final long weCookie;
    private final int recTypeCode;
    private final int recTypeLength;
    private final int warmUpType;
    private final long[] fileCookieParams;
    private final long[] buffAddresses;
    private final byte[] chunkHeader;
    private final ByteBuffersIndexOutput byteBuffersIndexOutput;

    public WarpIndexOutput(String fileName,
            WriteJuffersWarmUpElement juffersWE,
            StorageEngineConstants storageEngineConstants,
            StorageEngine storageEngine,
            long weCookie,
            int recTypeCode,
            int recTypeLength,
            int warmUpType,
            long[] fileCookieParams,
            long[] buffAddresses,
            byte[] chunkHeader)
    {
        super("WarpLuceneIndex", "warpLucene");
        this.storageEngine = storageEngine;
        this.luceneFileType = LuceneFileType.getType(fileName);
        this.storageEngineConstants = storageEngineConstants;
        this.weCookie = weCookie;
        this.recTypeCode = recTypeCode;
        this.recTypeLength = recTypeLength;
        this.warmUpType = warmUpType;
        this.fileCookieParams = fileCookieParams;
        this.buffAddresses = buffAddresses;
        this.chunkHeader = chunkHeader;
        this.juffersWE = juffersWE;
        this.byteBuffersIndexOutput = new ByteBuffersIndexOutput(new ByteBuffersDataOutput(), "WarpLuceneIndex", "warpLucene");
    }

    @Override
    public void copyBytes(DataInput input, long numBytes)
            throws IOException
    {
        if (numBytes > Integer.MAX_VALUE) {
            throw new RuntimeException("too large copy of " + numBytes + " bytes");
        }

        if (luceneFileType != LuceneFileType.UNKNOWN) {
            logger.debug("weCookie %x, LUCENE INDEX %s before copyBytes called %d", weCookie, super.getName(), numBytes);
            int bufferSize = luceneFileType.isSmallFile() ? storageEngineConstants.getLuceneSmallJufferSize() : storageEngineConstants.getLuceneBigJufferSize();
            writeToJuffer(bufferSize, input, (int) numBytes); // we enter this if only if number of bytes is lower than max integer
            logger.debug("weCookie %x, LUCENE INDEX %s after copyBytes called %d", weCookie, super.getName(), numBytes);
            return;
        }

        byteBuffersIndexOutput.copyBytes(input, numBytes);
    }

    private void writeToJuffer(int bufferSize, DataInput input, int numBytes)
    {
        ByteBuffer luceneFileBuffer = juffersWE.getLuceneFileBuffer(luceneFileType);
        int bytesLeftToRead = numBytes;
        int offset = 0;
        byte[] readBytes = new byte[bufferSize];

        while (bytesLeftToRead > 0) {
            int bytesToRead = Math.min(bufferSize, bytesLeftToRead);
            try {
                luceneFileBuffer.position(0);
                input.readBytes(readBytes, 0, bytesToRead);
                luceneFileBuffer.put(readBytes, 0, bytesToRead);
                logger.debug("weCookie %x, before write buffer file %s(%d), offset %d, length %d", weCookie, luceneFileType, luceneFileType.getNativeId(), offset, bytesToRead);
                long res = storageEngine.warmupLucene(weCookie,
                        luceneFileType.getNativeId(),
                        offset,
                        bytesToRead,
                        recTypeCode,
                        recTypeLength,
                        warmUpType,
                        fileCookieParams,
                        buffAddresses,
                        chunkHeader);
                fileCookieParams[FILE_COOKIE_PARAMS_START_OFFSET.ordinal()] = res & 0xFFFFFFFFL;
                fileCookieParams[FILE_COOKIE_PARAMS_WRITE_BUF_PAGE_IX.ordinal()] = res >> 32;
            }
            catch (IOException e) {
                logger.warn(e, "Failed to write buffer. weCookie %x", weCookie);
                throw new RuntimeException(e);
            }
            bytesLeftToRead -= bytesToRead;
            offset += bytesToRead;
        }
    }

    @Override
    public void close()
            throws IOException
    {
        byteBuffersIndexOutput.close();
    }

    @Override
    public long getFilePointer()
    {
        return byteBuffersIndexOutput.getFilePointer();
    }

    @Override
    public long getChecksum()
            throws IOException
    {
        return byteBuffersIndexOutput.getChecksum();
    }

    @Override
    public void writeByte(byte b)
            throws IOException
    {
        byteBuffersIndexOutput.writeByte(b);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length)
            throws IOException
    {
        byteBuffersIndexOutput.writeBytes(b, offset, length);
    }

    @Override
    public void writeBytes(byte[] b, int length)
            throws IOException
    {
        byteBuffersIndexOutput.writeBytes(b, length);
    }

    @Override
    public void writeInt(int i)
            throws IOException
    {
        byteBuffersIndexOutput.writeInt(i);
    }

    @Override
    public void writeShort(short i)
            throws IOException
    {
        byteBuffersIndexOutput.writeShort(i);
    }

    @Override
    public void writeLong(long i)
            throws IOException
    {
        byteBuffersIndexOutput.writeLong(i);
    }

    @Override
    public void writeString(String s)
            throws IOException
    {
        byteBuffersIndexOutput.writeString(s);
    }

    @Override
    public void writeMapOfStrings(Map<String, String> map)
            throws IOException
    {
        byteBuffersIndexOutput.writeMapOfStrings(map);
    }

    @Override
    public void writeSetOfStrings(Set<String> set)
            throws IOException
    {
        byteBuffersIndexOutput.writeSetOfStrings(set);
    }
}
