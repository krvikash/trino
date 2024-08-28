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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ChunkStateHandlerTest
{
    private ChunkStateHandler chunkStateHandler;

    @BeforeEach
    void setUp()
            throws IOException
    {
        StorageEngineConstants storageEngineConstants = mock(StorageEngineConstants.class);
        when(storageEngineConstants.getPageSize()).thenReturn(8192);
        when(storageEngineConstants.getPageOffsetMask()).thenReturn(8192 - 1);

        String rowGroupFilePath = "/tmp/testChunkStateHandler/schema/table/column/offset/length/123456";
        FileUtils.createParentDirectories(new File(rowGroupFilePath));

        int matchOffset = 0;

        chunkStateHandler = new ChunkStateHandler(storageEngineConstants, rowGroupFilePath, matchOffset);
    }

    @AfterEach
    void tearDown()
            throws IOException
    {
        FileUtils.deleteDirectory(new File("/tmp/testChunkStateHandler/"));
    }

    @Test
    void testChunkStateHandler()
    {
        List<ChunkState> chunkStatesIn = new ArrayList<>();

        for (int i = 0; i < 500; i++) {
            int[] filesLength = new int[LuceneFileType.numFiles()];

            for (int j = 0; j < LuceneFileType.numFiles(); j++) {
                filesLength[j] = i;
            }
            ChunkState chunkState = new ChunkState(i, i, filesLength);
            chunkStatesIn.add(chunkState);
        }

        int sizeInPages = chunkStateHandler.save(chunkStatesIn);
        Assertions.assertEquals(2, sizeInPages);

        List<ChunkState> chunkStatesOut = chunkStateHandler.load(0);
        Assertions.assertEquals(341, chunkStatesOut.size());
        for (int i = 0; i < 341; i++) {
            ChunkState chunkStateIn = chunkStatesIn.get(i);
            ChunkState chunkStateOut = chunkStatesOut.get(i);

            Assertions.assertEquals(chunkStateIn.startOffset(), chunkStateOut.startOffset());
            Assertions.assertEquals(chunkStateIn.readSize(), chunkStateOut.readSize());
            Assertions.assertArrayEquals(chunkStateIn.filesLength(), chunkStateOut.filesLength());
        }

        chunkStatesOut = chunkStateHandler.load(1);
        Assertions.assertEquals(500 - 341, chunkStatesOut.size());
        for (int i = 0; i < 500 - 341; i++) {
            ChunkState chunkStateIn = chunkStatesIn.get(341 + i);
            ChunkState chunkStateOut = chunkStatesOut.get(i);

            Assertions.assertEquals(chunkStateIn.startOffset(), chunkStateOut.startOffset());
            Assertions.assertEquals(chunkStateIn.readSize(), chunkStateOut.readSize());
            Assertions.assertArrayEquals(chunkStateIn.filesLength(), chunkStateOut.filesLength());
        }
    }
}
