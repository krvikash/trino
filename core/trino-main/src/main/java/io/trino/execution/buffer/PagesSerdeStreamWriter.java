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

import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.Slice;
import io.trino.annotation.NotThreadSafe;
import io.trino.spi.Page;
import io.trino.spi.PageStreamWriter;

import java.io.IOException;
import java.io.OutputStream;

import static java.util.Objects.requireNonNull;

@NotThreadSafe
class PagesSerdeStreamWriter
        implements PageStreamWriter
{
    private final PageSerializer serializer;
    private final OutputStreamSliceOutput output;

    private boolean closed;

    public PagesSerdeStreamWriter(OutputStream outputStream, PageSerializer serializer)
    {
        this.serializer = requireNonNull(serializer, "serializer is null");
        output = new OutputStreamSliceOutput(outputStream);
    }

    @Override
    public void writePage(Page page)
    {
        Slice serializedPage = serializer.serialize(page);
        output.writeBytes(serializedPage);
    }

    @Override
    public long getWrittenBytes()
    {
        return output.longSize();
    }

    @Override
    public void close()
            throws IOException
    {
        if (closed) {
            return;
        }
        closed = true;
        output.close();
    }
}
