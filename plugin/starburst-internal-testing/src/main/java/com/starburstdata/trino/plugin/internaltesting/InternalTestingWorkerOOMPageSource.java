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
package com.starburstdata.trino.plugin.internaltesting;

import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;

import static java.util.Objects.requireNonNull;

public class InternalTestingWorkerOOMPageSource
        implements ConnectorPageSource
{
    private final String oomTestString = "HEAP_DUMP_WORKER_TEST_SENSITIVE_STRING_DATA";
    private final String[] oomTestStringArray = {"HEAP_DUMP_WORKER_TEST_ARRAY_ELEMENT_1", "HEAP_DUMP_WORKER_TEST_ARRAY_ELEMENT_2"};
    private final InternalTestingMemoryAllocator memoryAllocation;
    private boolean closed;

    @Inject
    public InternalTestingWorkerOOMPageSource(InternalTestingMemoryAllocator memoryAllocation)
    {
        this.memoryAllocation = requireNonNull(memoryAllocation, "memoryAllocation is null");
    }

    public String getOOMTestString()
    {
        return oomTestString;
    }

    public String[] getOOMTestStringArray()
    {
        return oomTestStringArray;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return false;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        if (closed) {
            return null;
        }
        memoryAllocation.allocate();
        return SourcePage.create(Integer.MAX_VALUE);
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
        closed = true;
    }
}
