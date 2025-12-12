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
package io.trino.execution.buffer;

import com.google.common.collect.AbstractIterator;
import io.airlift.slice.Slice;
import io.trino.annotation.NotThreadSafe;
import io.trino.spi.Page;
import io.trino.spi.PageStreamReader;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.util.Iterator;

import static io.trino.execution.buffer.PagesSerdeUtil.readSerializedPages;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

@NotThreadSafe
class PagesSerdeStreamReader
        extends AbstractIterator<Page>
        implements PageStreamReader
{
    private final PageDeserializer deserializer;
    private final Iterator<Slice> pageIterator;
    private final InputStream inputStream;

    private boolean closed;

    public PagesSerdeStreamReader(InputStream inputStream, PageDeserializer deserializer)
    {
        this.deserializer = requireNonNull(deserializer, "deserializer is null");
        this.inputStream = requireNonNull(inputStream, "inputStream is null");
        pageIterator = readSerializedPages(inputStream);
    }

    @Override
    protected Page computeNext()
    {
        try {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedIOException();
            }

            if (pageIterator.hasNext()) {
                Slice slice = pageIterator.next();
                return deserializer.deserialize(slice);
            }

            return endOfData();
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to read page", e);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (closed) {
            return;
        }
        closed = true;
        inputStream.close();
    }
}
