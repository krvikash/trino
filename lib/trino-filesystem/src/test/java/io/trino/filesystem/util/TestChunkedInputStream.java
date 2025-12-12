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
package io.trino.filesystem.util;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.memory.MemoryFileSystem;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.IOException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestChunkedInputStream
{
    @Test
    public void testReadingMultipleChunksWithReadAllBytes()
            throws IOException
    {
        String fileContent = "some file context that will be split into multiple chunks";
        TrinoInputFile file = openFile(fileContent);
        ChunkedInputStream chunkedInputStream = new ChunkedInputStream(file.newInput(), file.length(), 7);
        assertThat(new String(chunkedInputStream.readAllBytes(), UTF_8)).isEqualTo(fileContent);
    }

    @Test
    public void testReadingMultipleChunksWithRead()
            throws IOException
    {
        String fileContent = "some file context that will be split into multiple chunks";
        TrinoInputFile file = openFile(fileContent);
        ChunkedInputStream chunkedInputStream = new ChunkedInputStream(file.newInput(), file.length(), 7);
        byte[] result = new byte[(int) file.length()];
        int i = 0;
        int b;
        while ((b = chunkedInputStream.read()) != -1) {
            result[i++] = (byte) b;
        }
        assertThat(new String(result, UTF_8)).isEqualTo(fileContent);
    }

    @Test
    public void testReadToBuffer()
            throws IOException
    {
        String fileContent = "some file context that will be split into multiple chunks";
        TrinoInputFile file = openFile(fileContent);
        ChunkedInputStream chunkedInputStream = new ChunkedInputStream(file.newInput(), file.length(), 7);
        byte[] buffer = new byte[10];
        assertThat(chunkedInputStream.read(buffer, 0, 0)).isEqualTo(0);

        assertThat(chunkedInputStream.read(buffer, 0, 2)).isEqualTo(2);
        assertThat(buffer[0]).isEqualTo(fileContent.getBytes(UTF_8)[0]);
        assertThat(buffer[1]).isEqualTo(fileContent.getBytes(UTF_8)[1]);
        assertThat(chunkedInputStream.read(buffer, 2, 1)).isEqualTo(1);
        assertThat(buffer[2]).isEqualTo(fileContent.getBytes(UTF_8)[2]);
    }

    @Test
    public void testSkip()
            throws IOException
    {
        String fileContent = "01234567890123456789";
        TrinoInputFile file = openFile(fileContent);
        ChunkedInputStream chunkedInputStream = new ChunkedInputStream(file.newInput(), file.length(), 7);
        assertThat(chunkedInputStream.skip(0)).isEqualTo(0);
        assertThat(chunkedInputStream.skip(7)).isEqualTo(7);
        assertThat(new String(chunkedInputStream.readNBytes(2), UTF_8)).isEqualTo("78");
        assertThat(chunkedInputStream.skip(4)).isEqualTo(4);
        assertThat(new String(chunkedInputStream.readAllBytes(), UTF_8)).isEqualTo("3456789");
        assertThat(chunkedInputStream.skip(1)).isEqualTo(0);
    }

    @Test
    public void testSkipNBytes()
            throws IOException
    {
        String fileContent = "01234567890123456789";
        TrinoInputFile file = openFile(fileContent);
        ChunkedInputStream chunkedInputStream = new ChunkedInputStream(file.newInput(), file.length(), 7);
        chunkedInputStream.skipNBytes(7);
        assertThat(new String(chunkedInputStream.readNBytes(2), UTF_8)).isEqualTo("78");
        chunkedInputStream.skipNBytes(4);
        assertThat(new String(chunkedInputStream.readAllBytes(), UTF_8)).isEqualTo("3456789");
        assertThatThrownBy(() -> chunkedInputStream.skipNBytes(1)).isInstanceOf(EOFException.class);
    }

    private static TrinoInputFile openFile(String fileContent)
            throws IOException
    {
        TrinoFileSystem fs = new MemoryFileSystem();
        Location location = Location.of("memory://").appendPath("text");
        fs.newOutputFile(location).createExclusive(fileContent.getBytes(UTF_8));
        return fs.newInputFile(location);
    }
}
