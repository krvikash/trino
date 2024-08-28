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

import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;
import java.util.Random;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LuceneIndexWriterReaderTest
{
    private final String rowGroupFilePath = "/tmp/testLuceneIndexWriterReader/schema/table/column/offset/length/123456";

    private StorageEngineConstants storageEngineConstants;
    private LuceneIndexWriter luceneIndexWriter;

    @BeforeEach
    void setUp()
            throws IOException
    {
        storageEngineConstants = mock(StorageEngineConstants.class);
        when(storageEngineConstants.getPageSize()).thenReturn(8192);
        when(storageEngineConstants.getPageOffsetMask()).thenReturn(8192 - 1);

        String[] fileNames = new String[LuceneFileType.numFiles()];
        String filePrefix = "_0";
        fileNames[LuceneFileType.SI.getFileId()] = filePrefix + ".si";
        fileNames[LuceneFileType.CFE.getFileId()] = filePrefix + ".cfe";
        fileNames[LuceneFileType.SEGMENTS.getFileId()] = "segments_1";
        fileNames[LuceneFileType.CFS.getFileId()] = filePrefix + ".cfs";

        IndexInput indexInputSmall = mock(IndexInput.class);
        when(indexInputSmall.length()).thenReturn(1000L);
        final int[] i = {0};
        doAnswer(invocation -> {
            byte[] bytes = invocation.getArgument(0);
//            int off = invocation.getArgument(1);
//            int len = invocation.getArgument(2);
//            new Random().nextBytes(bytes);
            Arrays.fill(bytes, 0, bytes.length, (byte) (99 - (i[0]++ * 11)));
            return null;
        }).when(indexInputSmall).readBytes(any(byte[].class), anyInt(), anyInt());

        IndexInput indexInputBig = mock(IndexInput.class);
        when(indexInputBig.length()).thenReturn(10000L);
        doAnswer(invocation -> {
            byte[] bytes = invocation.getArgument(0);
            new Random().nextBytes(bytes);
            return null;
        }).when(indexInputBig).readBytes(any(byte[].class), anyInt(), anyInt());

        Directory luceneDirectory = mock(Directory.class);
        when(luceneDirectory.listAll()).thenReturn(fileNames);
        when(luceneDirectory.openInput(anyString(), any(IOContext.class)))
                .thenAnswer(invocation -> {
                    String fileName = invocation.getArgument(0);
                    if (fileName.equals(fileNames[LuceneFileType.CFS.getFileId()])) {
                        return indexInputBig;
                    }
                    else {
                        return indexInputSmall;
                    }
                });

        IndexWriter indexWriter = mock(IndexWriter.class);
        when(indexWriter.getDirectory()).thenReturn(luceneDirectory);

        FileUtils.createParentDirectories(new File(rowGroupFilePath));

        int luceneOffset = 0;

        luceneIndexWriter = new LuceneIndexWriter(storageEngineConstants, indexWriter, rowGroupFilePath, luceneOffset);
    }

    @AfterEach
    void tearDown()
            throws IOException
    {
        FileUtils.deleteDirectory(new File("/tmp/testLuceneIndexWriterReader/"));
    }

    @Test
    void saveLuceneIndex()
            throws IOException
    {
        Optional<ChunkState> chunkState = luceneIndexWriter.saveLuceneIndex();

        Assertions.assertTrue(chunkState.isPresent());
        Assertions.assertEquals(0, chunkState.get().startOffset());
        Assertions.assertEquals(3, chunkState.get().readSize());
        int[] filesLength = {1000, 1000, 1000, 10000};
        Assertions.assertArrayEquals(filesLength, chunkState.get().filesLength());

        LuceneIndexReader luceneIndexReader = new LuceneIndexReader(storageEngineConstants, rowGroupFilePath, chunkState.get());

        ByteBuffer smallFile = luceneIndexReader.loadSmallFile(LuceneFileType.SI);
        Assertions.assertEquals(0, smallFile.position());
        Assertions.assertEquals(1000, smallFile.limit());
        Assertions.assertEquals(1000, smallFile.capacity());
        for (int i = 0; i < 1000; i++) {
            Assertions.assertEquals(99, smallFile.get(i));
        }

        smallFile = luceneIndexReader.loadSmallFile(LuceneFileType.CFE);
        Assertions.assertEquals(0, smallFile.position());
        Assertions.assertEquals(1000, smallFile.limit());
        Assertions.assertEquals(1000, smallFile.capacity());
        for (int i = 0; i < 1000; i++) {
            Assertions.assertEquals(88, smallFile.get(i));
        }

        smallFile = luceneIndexReader.loadSmallFile(LuceneFileType.SEGMENTS);
        Assertions.assertEquals(0, smallFile.position());
        Assertions.assertEquals(1000, smallFile.limit());
        Assertions.assertEquals(1000, smallFile.capacity());
        for (int i = 0; i < 1000; i++) {
            Assertions.assertEquals(77, smallFile.get(i));
        }

        ByteBuffer bigFile = luceneIndexReader.loadBigFilePage(0);
        Assertions.assertEquals(0, bigFile.position());
        Assertions.assertEquals(8192, bigFile.limit());
        Assertions.assertEquals(8192, bigFile.capacity());

        bigFile = luceneIndexReader.loadBigFilePage(1);
        Assertions.assertEquals(0, bigFile.position());
        Assertions.assertEquals(10000 - 8192, bigFile.limit());
        Assertions.assertEquals(10000 - 8192, bigFile.capacity());
    }
}
