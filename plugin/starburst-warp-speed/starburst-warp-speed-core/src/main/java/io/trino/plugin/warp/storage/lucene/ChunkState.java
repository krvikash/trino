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

import java.nio.ByteBuffer;
import java.util.Arrays;

public record ChunkState(int startOffset,   // start offset (in pages) of Lucene files of this chunk
                         int readSize,      // size (in pages) of all 4 Lucene files of this chunk
                         int[] filesLength) // length (in bytes) of each Lucene file of this chunk
{
    // startOffset + readSize + filesLength[LuceneFileType.numFiles()]
    private static final int NUM_FIELDS = 6;

    public int endOffset()
    {
        return startOffset + readSize;
    }

    public static int size()
    {
        return NUM_FIELDS * Integer.BYTES;
    }

    public void put(ByteBuffer byteBuffer)
    {
        byteBuffer.putInt(startOffset);
        byteBuffer.putInt(readSize);
        for (int i = 0; i < LuceneFileType.numFiles(); i++) {
            byteBuffer.putInt(filesLength[i]);
        }
    }

    public static ChunkState get(ByteBuffer byteBuffer)
    {
        int startOffset = byteBuffer.getInt();
        int readSize = byteBuffer.getInt();
        int[] filesLength = new int[LuceneFileType.numFiles()];

        for (int i = 0; i < LuceneFileType.numFiles(); i++) {
            filesLength[i] = byteBuffer.getInt();
        }
        return new ChunkState(startOffset, readSize, filesLength);
    }

    @Override
    public String toString()
    {
        return "ChunkState{" +
                "startOffset=" + startOffset +
                ", readSize=" + readSize +
                ", filesLength=" + Arrays.toString(filesLength) +
                '}';
    }
}
