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

import io.airlift.log.Logger;
import io.airlift.units.DataSize;

import java.util.ArrayList;
import java.util.List;

import static io.airlift.units.DataSize.Unit.GIGABYTE;

public final class InternalTestingMemoryAllocator
{
    private static final Logger log = Logger.get(InternalTestingMemoryAllocator.class);
    private static final DataSize OOM_ALLOCATION_SIZE = DataSize.of(100L, GIGABYTE);

    public List<byte[]> allocate()
    {
        long targetBytes = OOM_ALLOCATION_SIZE.toBytes();
        log.info("Starting allocating data of size %s".formatted(targetBytes));
        List<byte[]> blocks = new ArrayList<>();
        long allocated = 0;
        int batchNumber = 1;
        while (allocated < targetBytes) {
            log.info("Allocating next batch... [%s]".formatted(batchNumber));
            int blockSize = (int) Math.min(1024 * 1024, targetBytes - allocated); // 1MB chunks
            blocks.add(new byte[blockSize]);
            allocated += blockSize;
            batchNumber++;
        }
        return blocks;
    }
}
