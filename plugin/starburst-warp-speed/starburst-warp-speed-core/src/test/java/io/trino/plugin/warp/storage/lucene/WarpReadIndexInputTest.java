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

import io.trino.plugin.warp.gen.stats.LucenePageCacheStats;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import org.apache.lucene.store.IndexInput;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WarpReadIndexInputTest
{
    private static final int SMALL_FILE_SIZE = 20;

    private WarpReadIndexInput warpReadIndexInput;
    private LuceneIndexReader luceneIndexReader;
    private LucenePageCacheStats lucenePageCacheStats;

    @BeforeEach
    public void before()
            throws IOException
    {
        StorageEngineConstants storageEngineConstants = mock(StorageEngineConstants.class);
        when(storageEngineConstants.getPageSize()).thenReturn(SMALL_FILE_SIZE);

        luceneIndexReader = mock(LuceneIndexReader.class);
        when(luceneIndexReader.loadBigFilePage(anyInt())).thenReturn(allocateByteBuffer());

        lucenePageCacheStats = LucenePageCacheStats.create();

        warpReadIndexInput = new WarpReadIndexInput(luceneIndexReader,
                storageEngineConstants,
                lucenePageCacheStats,
                new ByteBuffer[LuceneFileType.numSmallFiles()],
                new HashMap<>(),
                -1,
                LuceneFileType.CFS,
                0,
                100,
                "root");
    }

    @Test
    public void testReadBytesLessThenPageSizeShouldFetchOnce()
            throws IOException
    {
        for (int i = 0; i < SMALL_FILE_SIZE - 1; i++) {
            warpReadIndexInput.readByte();
        }
        verify(luceneIndexReader, times(1)).loadBigFilePage(eq(0));
        assertThat(warpReadIndexInput.getBufferPosition()).isEqualTo(SMALL_FILE_SIZE - 1);
    }

    @Test
    public void testReadBytesMoreThenPageSizeShouldFetchWhenNeeded()
            throws IOException
    {
        for (int i = 0; i < SMALL_FILE_SIZE; i++) {
            warpReadIndexInput.readByte();
        }
        verify(luceneIndexReader, times(1)).loadBigFilePage(eq(0));

        warpReadIndexInput.readByte();
        verify(luceneIndexReader, times(1)).loadBigFilePage(eq(1));
        assertThat(warpReadIndexInput.getBufferPosition()).isEqualTo(1);
    }

    @Test
    public void testReadBytes()
            throws IOException
    {
        int numOfBytesToRead = 5;
        warpReadIndexInput.readInt();
        assertThat(warpReadIndexInput.getFilePointer()).isEqualTo(Integer.BYTES);
        assertThat(warpReadIndexInput.getBufferPosition()).isEqualTo(Integer.BYTES);

        byte[] readBytes = new byte[numOfBytesToRead];
        warpReadIndexInput.readBytes(readBytes, 0, numOfBytesToRead);
        verify(luceneIndexReader, times(1)).loadBigFilePage(eq(0));
        assertThat(warpReadIndexInput.getFilePointer()).isEqualTo(Integer.BYTES + numOfBytesToRead);
        assertThat(warpReadIndexInput.getBufferPosition()).isEqualTo(Integer.BYTES + numOfBytesToRead);
    }

    @Test
    public void testReadIntMoreThenPageSizeShouldFetchWhenNeeded()
            throws IOException
    {
        for (int i = 0; i < SMALL_FILE_SIZE / Integer.BYTES; i++) {
            warpReadIndexInput.readInt();
        }
        assertThat(warpReadIndexInput.getBufferPosition()).isEqualTo(SMALL_FILE_SIZE);

        verify(luceneIndexReader, times(1)).loadBigFilePage(eq(0));

        warpReadIndexInput.readInt();
        verify(luceneIndexReader, times(1)).loadBigFilePage(eq(1));
        assertThat(warpReadIndexInput.getBufferPosition()).isEqualTo(4);
        assertThat(warpReadIndexInput.getFilePointer()).isEqualTo(SMALL_FILE_SIZE + Integer.BYTES);
    }

    @Test
    public void testSeekShouldResetRecordBuffer()
            throws IOException
    {
        warpReadIndexInput.seek(10);
        assertThat(warpReadIndexInput.getBufferPosition()).isEqualTo(10);
        assertThat(warpReadIndexInput.getFilePointer()).isEqualTo(10);

        verify(luceneIndexReader, times(1)).loadBigFilePage(eq(0));
    }

    @Test
    public void testSkipBytes()
            throws IOException
    {
        int bytesToSkip = 10;
        warpReadIndexInput.readInt();
        warpReadIndexInput.skipBytes(bytesToSkip);
        assertThat(warpReadIndexInput.getBufferPosition()).isEqualTo(bytesToSkip + Integer.BYTES);
        assertThat(warpReadIndexInput.getFilePointer()).isEqualTo(bytesToSkip + Integer.BYTES);

        verify(luceneIndexReader, times(1)).loadBigFilePage(eq(0));
    }

    @Test
    public void testSkipBytesShouldFetchAndAlign()
            throws IOException
    {
        int skipSize = SMALL_FILE_SIZE - 1;
        warpReadIndexInput.readInt();
        warpReadIndexInput.skipBytes(skipSize);
        assertThat(warpReadIndexInput.getBufferPosition()).isEqualTo((skipSize + Integer.BYTES) % SMALL_FILE_SIZE);
        assertThat(warpReadIndexInput.getFilePointer()).isEqualTo(skipSize + Integer.BYTES);

        verify(luceneIndexReader, times(2)).loadBigFilePage(anyInt());
    }

    @Test
    public void testSlice()
            throws IOException
    {
        warpReadIndexInput.readInt();
        long beforeSliceFilePointer = warpReadIndexInput.getFilePointer();
        IndexInput slice = warpReadIndexInput.slice("stam desc", 3, 10);
        assertThat(slice.getFilePointer()).isEqualTo(0); // new slice init it filePointer
        assertThat(slice.length()).isEqualTo(10);
        slice.seek(5);
        assertThat(slice.getFilePointer()).isEqualTo(5);
        assertThat(warpReadIndexInput.getFilePointer()).isEqualTo(beforeSliceFilePointer); // slice operation should not impact original indexInput
    }

    @Test
    public void testSliceFromSlice()
            throws IOException
    {
        int firstOffset = 3;
        int secondOffset = 5;
        int firstSeek = 50;
        int secondSeek = 23;
        warpReadIndexInput.readInt();

        WarpReadIndexInput firstSlice = (WarpReadIndexInput) warpReadIndexInput.slice("stam desc", firstOffset, 80);
        assertThat(firstSlice.getFilePointer()).isEqualTo(0); // new firstSlice init it filePointer
        assertThat(firstSlice.length()).isEqualTo(80);
        firstSlice.seek(firstSeek);
        assertThat(firstSlice.getAbsolutePostion()).isEqualTo(firstSeek + firstOffset);
        assertThat(firstSlice.getFilePointer()).isEqualTo(firstSeek);
        assertThat(firstSlice.getBufferPosition()).isEqualTo(firstSeek % SMALL_FILE_SIZE + firstOffset);

        WarpReadIndexInput secondSlice = (WarpReadIndexInput) firstSlice.slice("second slice", secondOffset, 50);
        firstSlice.readByte();
        assertThat(firstSlice.getAbsolutePostion()).isEqualTo(firstSeek + firstOffset + 1);
        assertThat(firstSlice.getBufferPosition()).isEqualTo((firstSeek + firstOffset) % SMALL_FILE_SIZE + 1);
        assertThat(firstSlice.getFilePointer()).isEqualTo(firstSeek + 1);

        assertThat(secondSlice.length()).isEqualTo(50);
        secondSlice.readByte();
        assertThat(secondSlice.getFilePointer()).isEqualTo(1);
        secondSlice.seek(secondSeek);
        assertThat(secondSlice.getFilePointer()).isEqualTo(secondSeek);
        assertThat(secondSlice.getBufferPosition()).isEqualTo((secondSeek + firstOffset + secondOffset) % SMALL_FILE_SIZE);

        assertThat(secondSlice.getAbsolutePostion()).isEqualTo(secondSeek + firstOffset + secondOffset);

        // check that origin slice wasn't changed
        assertThat(firstSlice.getAbsolutePostion()).isEqualTo(firstSeek + firstOffset + 1);
        assertThat(firstSlice.getBufferPosition()).isEqualTo((firstSeek + firstOffset) % SMALL_FILE_SIZE + 1);
        assertThat(firstSlice.getFilePointer()).isEqualTo(firstSeek + 1);
    }

    @Test
    public void testClone()
            throws IOException
    {
        warpReadIndexInput.readInt();
        warpReadIndexInput.readInt();

        WarpReadIndexInput clone = (WarpReadIndexInput) warpReadIndexInput.clone();
        assertThat(clone.getFilePointer()).isEqualTo(warpReadIndexInput.getFilePointer());
        assertThat(clone.getBufferPosition()).isEqualTo(warpReadIndexInput.getBufferPosition());
        clone.readInt();
        assertThat(clone.getFilePointer()).isNotEqualTo(warpReadIndexInput.getFilePointer());
        assertThat(clone.getBufferPosition()).isNotEqualTo(warpReadIndexInput.getBufferPosition());
    }

    ByteBuffer allocateByteBuffer()
    {
        return ByteBuffer.allocate(100);
    }

    ByteBuffer[] allocateLuceneByteBuffers()
    {
        return IntStream.range(0, 4).mapToObj((_) -> allocateByteBuffer()).toList().toArray(new ByteBuffer[0]);
    }

    @Test
    void testLucenePageCache()
    {
        LucenePageCacheKey keyIn = new LucenePageCacheKey(-1, LuceneFileType.SI, 0);
        ByteBuffer valueIn = ByteBuffer.wrap("234567".getBytes(StandardCharsets.UTF_8));

        warpReadIndexInput.put(keyIn, valueIn);

        LucenePageCacheKey keyOut = new LucenePageCacheKey(-1, LuceneFileType.SI, 0);
        Optional<ByteBuffer> valueOut = warpReadIndexInput.get(keyOut);

        Assertions.assertTrue(valueOut.isPresent());
        Assertions.assertEquals(valueIn, valueOut.get());
        Assertions.assertEquals(0, valueOut.get().position());
        Assertions.assertEquals(1, lucenePageCacheStats.getlucene_page_cache_small_file_hit());
    }
}
