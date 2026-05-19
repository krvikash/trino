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
package io.trino.plugin.warp.storage.memory;

import io.airlift.units.DataSize;
import io.trino.plugin.warp.log.ShapingLogger;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.atomic.AtomicLong;

public class GcArena
        extends ArenaBase
{
    static final long MAX_ALLOCATED_BYTES = DataSize.of(200, DataSize.Unit.MEGABYTE).toBytes();

    private final Runnable limitFunc;

    public GcArena(
            Runnable limitFunc,
            AtomicLong numAllocatedBytes,
            ShapingLogger shapingLogger)
    {
        super(numAllocatedBytes, shapingLogger);
        this.arena = Arena.ofAuto();
        this.limitFunc = limitFunc;
    }

    @Override
    public MemorySegment allocate(long numBytes, long alignment)
    {
        MemorySegment result = super.allocate(numBytes, alignment);
        if (globalNumAlllocatedBytes.get() > MAX_ALLOCATED_BYTES) {
            limitFunc.run();
        }
        return result;
    }
}
