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

import com.google.common.annotations.VisibleForTesting;
import io.airlift.log.Logger;
import io.trino.plugin.warp.gen.stats.LucenePageCacheStats;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import org.apache.lucene.store.BufferedChecksum;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public class WarpReadIndexInput
        extends ChecksumIndexInput
        implements Cloneable
{
    private static final Logger logger = Logger.get(WarpReadIndexInput.class);

    private final LuceneIndexReader luceneIndexReader;
    private final StorageEngineConstants storageEngineConstants;
    private final LucenePageCacheStats lucenePageCacheStats;
    private final ByteBuffer[] smallFilePageCache;
    private final Map<LucenePageCacheKey, ByteBuffer> bigFilePageCache;
    private final int indexUniqueIdInRowGroup;
    private final LuceneFileType luceneFileType;
    private final int pageSize;
    private final Checksum digest;
    private final String logPrefix;
    private final long length;
    private final String sliceDescription;
    private final long sliceOffset;

    // concurrent slices of the same file modifying the position of the juffer so we must have local copy
    private ByteBuffer localCopyBuffer;
    private int currentPageIndex;
    private int bufferLength;

    WarpReadIndexInput(LuceneIndexReader luceneIndexReader,
            StorageEngineConstants storageEngineConstants,
            LucenePageCacheStats lucenePageCacheStats,
            ByteBuffer[] smallFilePageCache,
            Map<LucenePageCacheKey, ByteBuffer> bigFilePageCache,
            int indexUniqueIdInRowGroup,
            LuceneFileType luceneFileType,
            long sliceOffset,
            long length,
            String sliceDescription)
    {
        super("WarpReadIndexInput_" + indexUniqueIdInRowGroup + "_" + luceneFileType);
        this.luceneIndexReader = luceneIndexReader;
        this.storageEngineConstants = storageEngineConstants;
        this.lucenePageCacheStats = lucenePageCacheStats;
        this.smallFilePageCache = smallFilePageCache;
        this.bigFilePageCache = bigFilePageCache;
        this.indexUniqueIdInRowGroup = indexUniqueIdInRowGroup;
        this.luceneFileType = luceneFileType;
        this.length = length;
        this.sliceOffset = sliceOffset;
        this.sliceDescription = sliceDescription;
        this.digest = new BufferedChecksum(new CRC32());
        this.logPrefix = String.format("%d(%s_%s_%d-%d)", indexUniqueIdInRowGroup, luceneFileType, sliceDescription, sliceOffset, sliceOffset + length);
        this.pageSize = storageEngineConstants.getPageSize();
        currentPageIndex = (int) (sliceOffset / pageSize);
        setCurrentBuffer();
        localCopyBuffer.position((int) sliceOffset % pageSize);
    }

    @Override
    public long getChecksum()
    {
        return digest.getValue();
    }

    @Override
    public void close()
    {
    }

    @Override
    public long length()
    {
        return length;
    }

    @Override
    public byte readByte()
    {
        if (localCopyBuffer.position() == bufferLength) {
            nextPage();
        }
        byte b = localCopyBuffer.get();
        digest.update(b);
        return b;
    }

    @Override
    public void readBytes(byte[] dst, int offset, int len)
    {
        int iterationLen = len;
        int iterationOffset = offset;
        while (iterationLen > 0) {
            if (localCopyBuffer.position() == bufferLength) {
                nextPage();
            }

            int remainInBuffer = bufferLength - localCopyBuffer.position();
            int bytesToCopy = Math.min(iterationLen, remainInBuffer);
            byte[] localSrc = new byte[bytesToCopy];
            localCopyBuffer.get(localSrc, 0, bytesToCopy);
            System.arraycopy(localSrc, 0, dst, iterationOffset, bytesToCopy);
            iterationOffset += bytesToCopy;
            iterationLen -= bytesToCopy;
        }
        digest.update(dst, offset, len);
    }

    private void nextPage()
    {
        currentPageIndex++;
        setCurrentBuffer();
    }

    private void loadPage()
    {
        // read up to one page
        int pageAlignOffset = currentPageIndex * pageSize;
        int fetchedBytes = (int) Math.min(pageSize, (sliceOffset + length - pageAlignOffset));
        LucenePageCacheKey key = new LucenePageCacheKey(indexUniqueIdInRowGroup, luceneFileType, pageAlignOffset);
        Optional<ByteBuffer> page = get(key);

        if (page.isEmpty() || (page.get().limit() < fetchedBytes)) {
            ByteBuffer byteBuffer;

            try {
                if (key.isSmallFile()) {
                    byteBuffer = luceneIndexReader.loadSmallFile(luceneFileType);
                }
                else {
                    byteBuffer = luceneIndexReader.loadBigFilePage(currentPageIndex);
                }
            }
            catch (Exception e) {
                throw new RuntimeException("loadPage failed key " + key, e);
            }
            page = Optional.of(put(key, byteBuffer));
        }
        localCopyBuffer = page.orElseThrow(() -> new NoSuchElementException("LucenePageCache not found for key " + key));
    }

    /**
     * @return the position in the slice
     */
    @Override
    public long getFilePointer()
    {
        return ((long) currentPageIndex * pageSize + localCopyBuffer.position()) - sliceOffset;
    }

    long getAbsolutePostion()
    {
        return getFilePointer() + sliceOffset;
    }

    @Override
    public void seek(long pos)
            throws IOException
    {
        int newBufferIndex = (int) ((pos + sliceOffset) / pageSize);
        if (newBufferIndex != currentPageIndex) {
            // we seek'd to a different buffer:
            currentPageIndex = newBufferIndex;
            setCurrentBuffer();
        }

        localCopyBuffer.position((int) (pos + sliceOffset) % pageSize);
        // This is not >= because seeking to exact end of file is OK: this is where
        // you'd also be if you did a readBytes of all bytes in the file
        if (getFilePointer() > length()) {
            throw new EOFException("seek beyond EOF: pos=" + getFilePointer() + " vs length=" + length() + ": " + this);
        }
    }

    private void setCurrentBuffer()
    {
        loadPage();
        bufferLength = (int) Math.min(pageSize, (sliceOffset + length - ((long) currentPageIndex * pageSize)));
    }

    @Override
    public IndexInput slice(String sliceDescription, final long offset, final long sliceLength)
    {
        return new WarpReadIndexInput(luceneIndexReader,
                storageEngineConstants,
                lucenePageCacheStats,
                smallFilePageCache,
                bigFilePageCache,
                indexUniqueIdInRowGroup,
                luceneFileType,
                offset + sliceOffset,
                sliceLength,
                sliceDescription);
    }

    @Override
    public IndexInput clone()
    {
        IndexInput ret = new WarpReadIndexInput(luceneIndexReader,
                storageEngineConstants,
                lucenePageCacheStats,
                smallFilePageCache,
                bigFilePageCache,
                indexUniqueIdInRowGroup,
                luceneFileType,
                sliceOffset,
                length,
                sliceDescription);
        try {
            ret.seek(getFilePointer());
        }
        catch (IOException e) {
            logger.error(e, "txId=%s, failed clone", logPrefix);
            throw new RuntimeException(e);
        }
        return ret;
    }

    @VisibleForTesting
    int getBufferPosition()
    {
        return localCopyBuffer.position();
    }

    @VisibleForTesting
    ByteBuffer put(LucenePageCacheKey key, ByteBuffer value)
    {
        value.position(0); // sanity
        ByteBuffer page = ByteBuffer.allocate(value.limit());

        page.put(value);
        value.position(0);
        page.position(0);

        if (key.isSmallFile()) {
            smallFilePageCache[key.getFileId()] = page;
            lucenePageCacheStats.inclucene_page_cache_small_file_size();
        }
        else {
            bigFilePageCache.put(key, page);
            lucenePageCacheStats.setlucene_page_cache_big_file_size(bigFilePageCache.size());
        }
        return page.duplicate();
    }

    @VisibleForTesting
    Optional<ByteBuffer> get(LucenePageCacheKey key)
    {
        Optional<ByteBuffer> value = Optional.empty();
        ByteBuffer page;

        if (key.isSmallFile()) {
            page = smallFilePageCache[key.getFileId()];
            if (page != null) {
                value = Optional.of(page.duplicate());
                lucenePageCacheStats.inclucene_page_cache_small_file_hit();
            }
            else {
                lucenePageCacheStats.inclucene_page_cache_small_file_miss();
            }
        }
        else {
            page = bigFilePageCache.get(key);
            if (page != null) {
                value = Optional.of(page.duplicate());
                lucenePageCacheStats.inclucene_page_cache_big_file_hit();
            }
            else {
                lucenePageCacheStats.inclucene_page_cache_big_file_miss();
            }
        }
        return value;
    }
}
