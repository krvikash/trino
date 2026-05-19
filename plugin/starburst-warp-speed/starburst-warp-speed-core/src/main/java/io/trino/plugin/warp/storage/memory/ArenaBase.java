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

import io.trino.plugin.warp.log.ShapingLogger;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.atomic.AtomicLong;

public abstract class ArenaBase
{
    protected Arena arena;
    protected AtomicLong globalNumAlllocatedBytes;
    protected long numAllocatedBytes;
    protected ShapingLogger shapingLogger;

    public ArenaBase(
            AtomicLong numAllocatedBytes,
            ShapingLogger shapingLogger)
    {
        this.globalNumAlllocatedBytes = numAllocatedBytes;
        this.shapingLogger = shapingLogger;
    }

    public MemorySegment allocate(long numBytes, long alignment)
    {
        numAllocatedBytes += (numBytes + alignment);
        globalNumAlllocatedBytes.addAndGet(numBytes + alignment);
        return arena.allocate(numBytes, alignment);
    }
}
